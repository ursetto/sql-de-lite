(use srfi-69)

(define-record lru-cache ht head tail capacity deleter)
(define-record node prev next key value)

(define %make-lru-cache make-lru-cache)

(define (make-lru-cache capacity comparator #!key (on-delete #f))
  (let ((ht (make-hash-table comparator)))
    (%make-lru-cache ht #f #f capacity on-delete)))

(define (splice-out! x)
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
  (and-let* ((n (hash-table-ref/default (lru-cache-ht c) k #f)))
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
  (let ((old (hash-table-ref/default (lru-cache-ht c) k #f)))
    (if old
        (node-value-set! old v)
        (let ((new (make-node #f (lru-cache-head c) k v)))
          (unless (lru-cache-tail c)
            (lru-cache-tail-set! c new))
          (when (lru-cache-head c)
            (node-prev-set! (lru-cache-head c) new))
          (lru-cache-head-set! c new)
          (hash-table-set! (lru-cache-ht c) k new)))))

(define (lru-cache-delete! c k)
  (when (lru-cache-deleter c)
    ((lru-cache-deleter c) v)))

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




(define C (make-lru-cache 5 string=?))
(lru-cache-set! C "hi there" 'hi-there)
(eq? (lru-cache-ref C "hi there") 'hi-there)     ; #t
(eq? (node-value (lru-cache-head C)) 'hi-there)  ; #t

(lru-cache-set! C "bye there" 'bye-there)
(eq? (lru-cache-ref C "bye there") 'bye-there)   ; #t
(eq? (node-value (lru-cache-head C)) 'bye-there) ; #t
(eq? (lru-cache-ref C "hi there") 'hi-there)     ; #t 
(eq? (node-value (lru-cache-head C)) 'hi-there)  ; #t



(lru-cache-walk C (lambda (k v) (write (cons k v)) (newline)))

