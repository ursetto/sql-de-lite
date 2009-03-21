(use srfi-69)

;; FIXME: accessors must be updated to silently fail when cache capacity
;; is 0.

(declare
 (fixnum))

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

#|


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

|#
