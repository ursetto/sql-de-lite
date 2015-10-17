(module sql-de-lite-cache

(make-lru-cache
 lru-cache?
 lru-cache-size
 lru-cache-capacity
 lru-cache-add!           ; adds a new k,v pair in MRU position
 lru-cache-remove!        ; removes and returns entry by key; deleter not triggered
 lru-cache-flush!
 lru-cache-walk)

(import scheme chicken)

(define-record-type lru-cache
  (%make-lru-cache  head tail capacity comparator deleter size)
  lru-cache?
  (head lru-cache-head set-lru-cache-head!)
  (tail lru-cache-tail set-lru-cache-tail!)
  (capacity lru-cache-capacity)
  (comparator lru-cache-comparator)
  (deleter lru-cache-deleter)
  (size lru-cache-size set-lru-cache-size!))

(define (make-lru-cache capacity comparator #!optional deleter)
  (%make-lru-cache #f #f capacity comparator deleter 0))

(define-record-type lru-entry
  (make-lru-entry prev next key value)
  lru-entry?
  (prev lru-entry-prev set-lru-entry-prev!)
  (next lru-entry-next set-lru-entry-next!)
  (key lru-entry-key)
  (value lru-entry-value))

(define-syntax while
  (syntax-rules ()
    ((while test body ...)
     (let loop ()
       (if test
           (begin body ...
                  (loop)))))))

(define (lru-cache-shrink! c)
  (let ((delete (lru-cache-deleter c)))
    (while (> (lru-cache-size c)
              (lru-cache-capacity c))
      (let ((L (lru-cache-dequeue! c)))
        (when delete
          (delete (car L) (cdr L)))))))

(define (lru-cache-enqueue! c k v)
  (let ((h (lru-cache-head c)))
    (if h
        (let ((e (make-lru-entry #f h k v)))
          (set-lru-entry-prev! h e)
          (set-lru-cache-head! c e))
        (let ((e (make-lru-entry #f #f k v)))
          (set-lru-cache-head! c e)
          (set-lru-cache-tail! c e)))
    (set-lru-cache-size! c (+ (lru-cache-size c) 1))))

(define (lru-cache-remove-entry! c e)
  (let ((p (lru-entry-prev e))
        (n (lru-entry-next e)))
    (if p
        (set-lru-entry-next! p n)
        (set-lru-cache-head! c n))
    (if n
        (set-lru-entry-prev! n p)
        (set-lru-cache-tail! c p)))  
  (set-lru-cache-size! c (- (lru-cache-size c) 1))
  e)

(define (lru-cache-find-entry c k)
  (do ((e (lru-cache-head c) (lru-entry-next e)))
      ((or (not e)
           ((lru-cache-comparator c) k (lru-entry-key e)))
       e)))

(define (lru-cache-dequeue! c)
  (let ((t (lru-cache-remove-entry! c (lru-cache-tail c))))
    (cons (lru-entry-key t)
          (lru-entry-value t))))

(define (lru-cache-walk c proc)
  (let ((h (lru-cache-head c)))
    (do ((e h (lru-entry-next e)))
        ((not e))
      (proc (lru-entry-key e)
            (lru-entry-value e)))))

(define (lru-cache-add! c k v)
  (cond ((> (lru-cache-capacity c) 0)
         (lru-cache-enqueue! c k v)
         (lru-cache-shrink! c)
         #t)
        (else #f)))

;; returns (k . v) pair or #f
(define (lru-cache-remove! c k)
  (and-let* ((e (lru-cache-find-entry c k)))
    (lru-cache-remove-entry! c e) ; also returns e
    (cons (lru-entry-key e)
          (lru-entry-value e))))

;; flushes in lru->mru order, but flush order not defined
(define (lru-cache-flush! c)
  (let ((delete (lru-cache-deleter c)))
    (do ((e (lru-cache-tail c) (lru-entry-prev e)))
        ((not e))
      (when delete
        (delete (lru-entry-key e)
                (lru-entry-value e)))))
  (set-lru-cache-size! c 0)
  (set-lru-cache-head! c #f)
  (set-lru-cache-tail! c #f))

)
