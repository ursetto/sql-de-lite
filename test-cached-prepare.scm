(use sqlite3-simple)
(use miscmacros)

;; Test cached statement performance.  Tests below are normalized to
;; run in about the same amount of time (about 0.45 seconds).

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
      (or (hash-table-ref sql cache)
          (and-let* ((stmt (prepare db sql)))
            (hash-table-set! cache sql stmt)
            stmt)))))

;; tested with schema:
;; create table cache(key text primary key, val text);

(call-with-database "a.db"
   (lambda (db)
     ;; fill the cache with one useful then 50 dummy statements
     (prepare/cached db "select * from cache;")
     (time (dotimes (i 50)
               (prepare/cached db (sprintf "select ~A;" i))))

     ;; select from front of alist
     (time (repeat 1000000 (prepare/cached db "select * from cache; ")))
     ;; select from back of alist (scan 50 statements)
     (time (repeat  130000 (prepare/cached db "select * from cache;")))
     ;; prepare statement where schema must be checked (small schema)
     (time (repeat   28000
                   (finalize (prepare db "select * from cache;"))))
     ;; prepare statement not requiring schema
     (time (repeat   45000
                   (finalize (prepare db "select 1;"))))

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
