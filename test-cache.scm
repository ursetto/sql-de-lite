(use srfi-1)
(define selects
  (map (lambda (x) (sprintf "select ~A;" x)) (iota 200)))

(define cache-hash
  (map (lambda (x)
         (cons (hash x)
               (cons x (conc "** " x " **"))))
       selects))

(define cache-string
  (map (lambda (x)
         (cons x (conc "** " x " **")))
       selects))

(define cache-hash-table
  (let ((ht (make-hash-table string=?)))
    (for-each (lambda (x)
                (hash-table-set! ht x (conc "** " x " **")))
              selects)
    ht))

(use miscmacros)
(use lru-cache)

(print "alist lookup by hashed value (end of 50 elt list)")
(print (cdr (alist-ref (string-hash "select 49;") cache-hash)))
(time (dotimes (i 1000000)
               (let ((s "select 49;"))
                 (and-let* ((cell (alist-ref (string-hash s) cache-hash)))
                   (and (string=? (car cell) s)
                        (cdr cell))))))

(print "hash-table lookup by string=?")
(print (hash-table-ref/default cache-hash-table "select 49;" #f))
(time (dotimes (i 1000000)
               (hash-table-ref/default cache-hash-table "select 49;" #f)))

;; Slower than hash table.  How is that possible?
(print "alist lookup by hashed value (first element)")
(print (cdr (alist-ref (string-hash "select 0;") cache-hash)))
(time (dotimes (i 1000000)
               (let ((s "select 0;"))
                 (and-let* ((cell (alist-ref (string-hash s) cache-hash)))
                   (and (string=? (car cell) s)
                        (cdr cell))))))

(print "(hash \"select 0;\")")
(time (dotimes (i 1000000)
               (let ((s "select 0;"))
                 (hash s))))

(print "(string-hash \"select 0;\")")
(time (dotimes (i 1000000)
               (let ((s "select 0;"))
                 (string-hash s))))

(print "alist lookup by hashed value (100th element)")
(print (cdr (alist-ref (string-hash "select 99;") cache-hash)))
(time (dotimes (i 1000000)
               (let ((s "select 99;"))
                 (and-let* ((cell (alist-ref (string-hash s) cache-hash)))
                   (and (string=? (car cell) s)
                        (cdr cell))))))

(print "alist lookup by hashed value (200th element)")
(print (cdr (alist-ref (string-hash "select 199;") cache-hash)))
(time (dotimes (i 1000000)
               (let ((s "select 199;"))
                 (and-let* ((cell (alist-ref (string-hash s) cache-hash)))
                   (and (string=? (car cell) s)
                        (cdr cell))))))

(print "hash-table lookup by string=? (200th elt)")
(print (hash-table-ref/default cache-hash-table "select 199;" #f))
(time (dotimes (i 1000000)
               (hash-table-ref/default cache-hash-table "select 199;" #f)))

;;; lru

(print "lru-cache populate 100 (capacity 100)")
(define C (make-lru-cache 100 string=?))
(time (for-each (lambda (x)
                  (lru-cache-set! C x x))
                selects))               ; should split up, half and half
(print "lru-cache lookup, select 100;")
(print (lru-cache-ref C "select 100;"))
(time (dotimes (i 1000000)
               (lru-cache-ref C "select 100;")))

(define v (list->vector selects))
(print "random number overhead")
(time (dotimes (i 1000000)
               (vector-ref v (random 200))))
(print "lru-cache lookup, random (half in cache)")
(print (lru-cache-ref C (vector-ref v (random 200))))
(time (dotimes (i 1000000)
               (lru-cache-ref C (vector-ref v (random 200)))))
(print "lru-cache lookup, random (all in cache)")
(print (lru-cache-ref C (vector-ref v (+ 100 (random 100)))))
(time (dotimes (i 1000000)
               (lru-cache-ref C (vector-ref v (+ 100 (random 100))))))
(print "lru-cache lookup of static uncached element")
(print (lru-cache-ref C "abcdef;"))
(time (dotimes (i 1000000)
               (lru-cache-ref C "abcdef;")))
(print "lru-cache lookup of static uncached element plus random # overhead")
(print (lru-cache-ref C "abcdef;"))
(time (dotimes (i 1000000)
               (vector-ref v (random 200))
               (lru-cache-ref C "abcdef;")))

(print "cache size: " (lru-cache-size C)) ; 100
;; (lru-cache-walk C (lambda (k v)
;;                     (write (cons k v)) (write " ")))

(when #f
  (print "lru-cache lookup 2 (experimental), random (half in cache)")
  (print (lru-cache-ref-2 C (vector-ref v (random 200))))
  (time (dotimes (i 1000000)
                 (lru-cache-ref-2 C (vector-ref v (random 200)))))
;; (lru-cache-walk C (lambda (k v)
;;                     (write (cons k v)) (write " ")))
  (print "lru-cache lookup 2 (experimental), random (all in cache)")
  (print (lru-cache-ref-2 C (vector-ref v (+ 100 (random 100)))))
  (time (dotimes (i 1000000)
                 (lru-cache-ref-2 C (vector-ref v (+ 100 (random 100))))))
  (print "cache size: " (lru-cache-size C))  ; 100
;; (lru-cache-walk C (lambda (k v)
;;                     (write (cons k v)) (write " ")))
  )


(print "alist lookup by hashed value, size 200, random lookup")
(print (cdr (alist-ref (string-hash (vector-ref v (random 200)))
                       cache-hash)))
(time (dotimes (i 1000000)
               (let ((s (vector-ref v (random 200))))
                 (and-let* ((cell (alist-ref (string-hash s) cache-hash)))
                   (and (string=? (car cell) s)
                        (cdr cell))))))
(print "alist lookup by hashed value, size 200, random lookup in first half")
(print (cdr (alist-ref (string-hash (vector-ref v (random 100)))
                       cache-hash)))
(time (dotimes (i 1000000)
               (let ((s (vector-ref v (random 100))))
                 (and-let* ((cell (alist-ref (string-hash s) cache-hash)))
                   (and (string=? (car cell) s)
                        (cdr cell))))))
;; unused
(define C (make-lru-cache 100 string=?
                          (lambda (k v)
                            (printf "deleting (~S ~S)\n" k v))))


;;; unusable

;; (print "alist lookup by string=? (end of 50 elt list)")
;; (print (alist-ref "select 49;" cache-string string=?))
;; (time (dotimes (i 1000000)
;;                (alist-ref "select 49;" cache-string string=?)))


;; (print "alist lookup by string=? (200th elt)")
;; (print (alist-ref "select 199;" cache-string string=?))
;; (time (dotimes (i 1000000)
;;                (alist-ref "select 199;" cache-string string=?)))

