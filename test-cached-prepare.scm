(use sqlite3-simple)
(use miscmacros)

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

;; TODO: prepare/cached/lru -- alist where recently-used statements move
;; to the front.  Probably only useful if the cache size is fixed; we
;; would have to finalize statements as they drop off, and each statement
;; would have to be checked for preparedness at execute time.  Note:
;; I believe statements could disappear from under us during a STEP in
;; the pathological case.  Unless statements are always prepared
;; opportunistically at execute time; then this problem would not occur
;; unless you have > 100 simultaneous running queries.  However, the
;; statement has to be prepared for us to get data on number of parameters
;; and column count.

;; tested with schema:
;; create table cache(key text primary key, val text);

(call-with-database "a.db"
   (lambda (db)
     ;; fill the cache with one useful then 50 dummy statements
     (prepare/cached db "select * from cache;")
     (dotimes (i 50)
              (prepare/cached db (sprintf "select ~A;" i)))

     ;; select from front of alist
     (time (repeat 1000000 (prepare/cached db "select * from cache; ")))
     ;; select from back of alist (scan 50 statements)
     (time (repeat  140000 (prepare/cached db "select * from cache;")))
     ;; prepare statement where schema must be checked (small schema)
     (time (repeat   30000
                     (finalize (prepare db "select * from cache;"))))
     ;; prepare statement not requiring schema
     (time (repeat   47000
                     (finalize (prepare db "select 1;"))))

;;; rerun for hash cache

     (print ":::::::::::::::::::::")

     ;; fill the hash cache with one useful then 50 dummy statements
     (prepare/cached/ht db "select * from cache;")
     (dotimes (i 50)
              (prepare/cached/ht db (sprintf "select ~A;" i)))
     ;; Select random statements.
     (time (repeat   820000 (prepare/cached/ht db "select * from cache; ")))
     (time (repeat   830000 (prepare/cached/ht db "select * from cache;")))
     (time (repeat   880000 (prepare/cached/ht db "select 20;")))
     
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
