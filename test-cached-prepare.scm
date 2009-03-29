(use sql-de-lite)
(use miscmacros)
(use lru-cache)

;; Test cached statement performance.  Tests below are normalized to
;; run in about the same amount of time (about 0.47 seconds).

(define prepare/cached
  (let ((cache '()))
    (lambda (db sql)
      (or (alist-ref sql cache string=?)
          (and-let* ((stmt (prepare db sql)))
            (set! cache (cons (cons sql stmt)
                              cache))
            stmt)))))

(define prepare/cached/ht
  (let ((cache (make-hash-table string=?)))
    (lambda (db sql)
      (or (hash-table-ref/default cache sql #f)
          (and-let* ((stmt (prepare db sql)))
            (hash-table-set! cache sql stmt)
            stmt)))))

(define lru-cache #f)
(define prepare/cached/lru
  (let ((cache (make-lru-cache 100 string=? (lambda (sql stmt)
                                              (finalize stmt)))))
    (set! lru-cache cache)
    (lambda (db sql)
      (or (lru-cache-ref cache sql)  ; note: reset not done
          (and-let* ((stmt (prepare-transient db sql)))
            (lru-cache-set! cache sql stmt)
            stmt)))))

;; tested with schema:
;; create table cache(key text primary key, val text);

(call-with-database "a.db"
   (lambda (db)
     ;; fill the cache with one useful then 50 dummy statements
     (prepare/cached db "select * from cache;")
     (dotimes (i 50)
              (prepare/cached db (sprintf "select ~A;" i)))

     ;; select from front of alist
     (time (repeat  950000 (prepare/cached db "select * from cache; ")))
     ;; select from back of alist (scan 50 statements)
     (time (repeat  135000 (prepare/cached db "select * from cache;")))
     ;; prepare statement where schema must be checked (small schema)
     (time (repeat   31000
                     (finalize (prepare-transient db "select * from cache;"))))
     ;; prepare statement not requiring schema
     (time (repeat   47000
                     (finalize (prepare-transient db "select 1;"))))
     ;; -- After upgrading to sql-de-lite, speed penalty (800000 -> 480000).
     ;; -- It's because statement is now reset when it is pulled from cache,
     ;; -- which is not accounted for in other tests.
     ;; cached prepare statement where schema must be checked (small schema)
     (time (repeat  480000
                     (prepare db "select * from cache;")))
     ;; cached prepare statement not requiring schema
     (time (repeat  520000
                     (prepare db "select 1;")))
     
;;; rerun for hash cache

     (print ":::::::::::::::::::::")

     ;; fill the hash cache with one useful then 50 dummy statements
     (prepare/cached/ht db "select * from cache;")
     (dotimes (i 50)
              (prepare/cached/ht db (sprintf "select ~A;" i)))
     ;; Select random statements.
     (time (repeat   950000 (prepare/cached/ht db "select * from cache; ")))
     (time (repeat   950000 (prepare/cached/ht db "select * from cache;")))
     (time (repeat  1050000 (prepare/cached/ht db "select 20;")))

;;; rerun for lru cache

     (print ":::::::::::::::::::::")

     (dotimes (i 50)
              (prepare/cached/lru db (sprintf "select ~A;" i)))
     ;; Read MRU statement repeatedly.
     (time (repeat  910000 (prepare/cached/lru db "select 0;")))
     ;; Alternate between two statements, simulating a random
     ;; lookup (LRU structure must be updated).
     (time (dotimes (i 700000)
                    (prepare/cached/lru db (if (fx= (fxand i 1) 1)
                                               "select 0;"
                                               "select 1;"))))   
     (print "-- alternate between two statements, native egg cache")
     (time (dotimes (i 430000)
                    (prepare db (if (fx= (fxand i 1) 1)
                                    "select 0;"
                                    "select 1;"))))   


     (lru-cache-flush! lru-cache)
     
     )
   )

#|

;; Confirms that string-append s allocates a new string
(let ((s "select * from cache;"))
      (print "s = (string-append s): " (eq? s (string-append s)))
      (print "(string-append s) = (string-append s): " (eq? (string-append s)
                                                          (string-append s))))

(print "literal strings eq?: " (eq? "select;" "select;"))

(let ((s ("static;")))
  (string-set! s 0 #\t))

|#
