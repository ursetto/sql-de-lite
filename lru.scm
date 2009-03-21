(use srfi-69)

;; FIXME: accessors must be updated to silently fail when cache capacity
;; is 0.

(declare
 (fixnum)
 (usual-integrations)
 (no-bound-checks)
 (no-procedure-checks-for-usual-bindings)
 (export
  make-lru-cache
  lru-cache-ref
  lru-cache-ref-2
  lru-cache-set!
  lru-cache-walk
  lru-cache-delete!
  lru-cache-flush!
  lru-cache-size
  )
 )

(cond-expand
 (compiling)
 (else
  (define-syntax define-inline
    (syntax-rules () ((_ args ...) (define args ...))))))

(define-record lru-cache ht head tail capacity deleter)
(define-record node prev next key value)

(define %make-lru-cache make-lru-cache)

(define (make-lru-cache capacity comparator #!key (on-delete #f))
  (let ((ht (make-hash-table comparator)))
    (%make-lru-cache ht #f #f capacity on-delete)))

(define-inline (lookup c k)
  (hash-table-ref/default (lru-cache-ht c) k #f))

(define-inline (splice-out! x)
  (let ((p (node-prev x))
        (n (node-next x)))
    (when p
      (node-next-set! p n)
      (node-prev-set! x #f))
    (when n
      (node-prev-set! n p)
      (node-next-set! x #f))
    n))

(define (lru-cache-ref c k)
  (and-let* ((n (lookup c k)))
    (if (not (node-prev n))             ; mru
        (node-value n)
        (begin
          (when (eq? n (lru-cache-tail c))
            (lru-cache-tail-set! c (node-prev n)))
          (splice-out! n)
          (let ((head (lru-cache-head c)))
            (node-prev-set! head n)
            (node-next-set! n head)
            (lru-cache-head-set! c n)
            (node-value n)
            )))))

;; This is, counterintuitively, substantially faster.  Mutations
;; are reduced to less than 2% of the node-mutating code.
(define (lru-cache-ref-2 c k)
  (and-let* ((n (lookup c k)))
    (if (not (node-prev n))             ; mru
        (node-value n)
        (begin
          (let ((pr (node-prev n))
                (nx (node-next n))
                (ht (lru-cache-ht c)))
            (when pr
              (let ((pr* (make-node
                          (node-prev pr)
                          nx
                          (node-key pr)
                          (node-value pr))))
                (hash-table-set! ht (node-key pr) pr*)
                (when (eq? n (lru-cache-tail c))
                  (lru-cache-tail-set! c pr*))))

            (when nx
              (hash-table-set! ht (node-key nx)
                               (make-node
                                pr
                                (node-next nx)
                                (node-key nx)
                                (node-value nx))))
            (let* ((head (lru-cache-head c))
                   (v (node-value n))
                   (new (make-node #f head k v)))
              (hash-table-set! ht k new)
              (hash-table-set! ht (node-key head)
                               (make-node
                                new
                                (node-next head)
                                (node-key head)
                                (node-value head)))
              (lru-cache-head-set! c new)
              v))))))

(define (lru-cache-set! c k v)
  (let ((old (lookup c k)))
    (if old
        (node-value-set! old v)
        (let ((new (make-node #f (lru-cache-head c) k v)))
          (when (>= (lru-cache-size c)
                    (lru-cache-capacity c))  ; FIXME assert difference is 0
            (lru-cache-delete! c (node-key (lru-cache-tail c))))
          (unless (lru-cache-tail c)
            (lru-cache-tail-set! c new))
          (when (lru-cache-head c)
            (node-prev-set! (lru-cache-head c) new))
          (lru-cache-head-set! c new)
          (hash-table-set! (lru-cache-ht c) k new)))))

;; Missing association is not an error.
(define (lru-cache-delete! c k)
  (and-let* ((n (lookup c k)))
    (hash-table-delete! (lru-cache-ht c) k)
    (when (eq? n (lru-cache-tail c))
      (lru-cache-tail-set! c (node-prev n)))
    (when (eq? n (lru-cache-head c))
      (lru-cache-head-set! c (node-next n)))
    (splice-out! n)
    (if (lru-cache-deleter c)
        ((lru-cache-deleter c) k (node-value n))
        #t)))

(define (lru-cache-size c)
  (hash-table-size (lru-cache-ht c)))

;; (define (lru-cache-fold c kons knil)
;;   (do ((n (lru-cache-head n) (node-next n)))
;;       ((not (node-next n)) )
;;       )
;;   (kons )
  
;;   (lru-cache-head c)
  

;;   )

(define (lru-cache-walk c proc)
  (do ((n (lru-cache-head c) (node-next n)))
      ((not n))
    (proc (node-key n) (node-value n))))

(define (lru-cache-flush! c)
  (lru-cache-walk c
                  (lambda (k v)
                    ((lru-cache-deleter c) k v)))
  (let ((ht (lru-cache-ht c)))
    (lru-cache-ht-set! c (make-hash-table (lru-cache-capacity c)
                                          (hash-table-equivalence-function ht)))
    (lru-cache-head-set! c #f)
    (lru-cache-tail-set! c #f)))


#|

(define (walk) (lru-cache-walk C (lambda (k v) (write (cons k v)) (newline))))
(define C (make-lru-cache 5 string=?))
(lru-cache-set! C "hi there" 'hi-there)
(eq? (lru-cache-ref C "hi there") 'hi-there)     ; #t
(eq? (node-value (lru-cache-head C)) 'hi-there)  ; #t

(lru-cache-set! C "bye there" 'bye-there)
(eq? (lru-cache-ref C "bye there") 'bye-there)   ; #t
(eq? (node-value (lru-cache-head C)) 'bye-there) ; #t
(eq? (lru-cache-ref C "hi there") 'hi-there)     ; #t 
(eq? (node-value (lru-cache-head C)) 'hi-there)  ; #t


(lru-cache-set! C "a" 1)
(lru-cache-set! C "b" 2)
(lru-cache-set! C "c" 3)
(lru-cache-ref C "hi there")
(lru-cache-walk C (lambda (k v) (write (cons k v)) (newline)))
     ;; ("hi there" . hi-there)
     ;; ("c" . 3)
     ;; ("b" . 2)
     ;; ("a" . 1)
     ;; ("bye there" . bye-there)

(lru-cache-set! C "d" 4)   ; should roll off "bye there"
(lru-cache-set! C "e" 5)   ; should roll off "a"
(lru-cache-set! C "b" 3)   ; should change value of b to 3, does not alter order


;;; stress test

(use miscmacros)
(define ss
  (list "a" "b" "c" "d" "e" "f" "g"))
(define vs (list->vector ss))

(define C2 (make-lru-cache 5 string=?))
(define (walk C) (lru-cache-walk C (lambda (k v) (write (cons k v)) (newline))))

(for-each (lambda (x)
            (lru-cache-set! C2 x (conc "*" x "*")))
          ss)
(walk C2)

(repeat 10
        (let ((s (vector-ref vs (random (vector-length vs)))))
          (lru-cache-ref-2 C2 (vector-ref vs (random (vector-length vs))))
          (print "-- chose " s)
          (walk C2)))
(walk C2)


|#
