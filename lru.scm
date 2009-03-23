;;; lru-cache

(use srfi-69)

(declare
 (fixnum)
 (usual-integrations)
 (no-bound-checks)
 (no-procedure-checks-for-usual-bindings)
 (bound-to-procedure
  ##sys#slot ##sys#setslot)
 )

(cond-expand
 (compiling)
 (else
  (define-syntax define-inline
    (syntax-rules () ((_ args ...) (define args ...))))))

(module lru-cache
  (make-lru-cache
   lru-cache-ref
   lru-cache-set!
   lru-cache-walk
   lru-cache-fold
   lru-cache-delete!
   lru-cache-flush!
   lru-cache-size
   lru-cache-capacity)

  (import scheme chicken srfi-69)
  (import record-variants)

  (define-record lru-cache ht head tail capacity deleter)
  (define-record-variant (%lru-cache lru-cache)
    (unsafe unchecked inline)
    ht head tail capacity deleter)

  (define-record lru-node prev next key value)  ; avoid record name conflict
  (define-record-variant (%node lru-node)
    (unsafe unchecked inline)
    prev next key value)

  (define make-lru-cache
    (let ((%make-lru-cache make-lru-cache))
      (lambda (capacity comparator #!optional (on-delete #f))
        (let ((ht (make-hash-table comparator)))
          (%make-lru-cache ht #f #f capacity on-delete)))))

  (define-inline (lookup c k)
    (hash-table-ref/default (lru-cache-ht c) k #f))
  (define-inline (%lookup c k)
    (hash-table-ref/default (%lru-cache-ht c) k #f)) ; dangerous
  (define (lru-cache-size c)
    (hash-table-size (lru-cache-ht c)))
  (define-inline (%lru-cache-size c)               ; dangerous
    (hash-table-size (%lru-cache-ht c)))

  (define (lru-cache-ref c k)
    (and
     (> (lru-cache-capacity c) 0)
     (and-let* ((n (%lookup c k)))
       (check-%node n)
       (if (not (%node-prev n))           ; MRU
           (%node-value n)
           (let ((nx (%node-next n))
                 (pr (%node-prev n)))
             (when pr
               (check-%node pr)
               (%node-next-set! pr nx)
               (%node-prev-set! n #f)
               (when (eq? n (%lru-cache-tail c))
                 (%lru-cache-tail-set! c pr)))
             (when nx
               (check-%node nx)
               (%node-prev-set! nx pr))
             (let ((head (%lru-cache-head c)))
               (check-%node head)
               (%node-prev-set! head n)
               (%node-next-set! n head)
               (%lru-cache-head-set! c n)
               (%node-value n)) )))))

  (define (lru-cache-set! c k v)
    (and
     (> (lru-cache-capacity c) 0)
     (let ((old (%lookup c k)))
       (if old
           (lru-node-value-set! old v)
           (let ((new (make-%node #f (%lru-cache-head c) k v)))
             (when (>= (%lru-cache-size c)
                       (%lru-cache-capacity c)) ; FIXME assert difference is 0
               (lru-cache-delete! c (lru-node-key (%lru-cache-tail c))))
             (unless (%lru-cache-tail c)
               (%lru-cache-tail-set! c new))
             (when (%lru-cache-head c)
               (lru-node-prev-set! (%lru-cache-head c) new))
             (%lru-cache-head-set! c new)
             (hash-table-set! (%lru-cache-ht c) k new))))))

  ;; Missing association is not an error.
  (define (lru-cache-delete! c k)
    (and
     (> (lru-cache-capacity c) 0)
     (and-let* ((n (%lookup c k)))
       (check-%node n)
       (hash-table-delete! (%lru-cache-ht c) k)
       (when (eq? n (%lru-cache-tail c))
         (%lru-cache-tail-set! c (%node-prev n)))
       (when (eq? n (%lru-cache-head c))
         (%lru-cache-head-set! c (%node-next n)))
       (let ((nx (%node-next n))
             (pr (%node-prev n)))
         (when pr (lru-node-next-set! pr nx))
         (when nx (lru-node-prev-set! nx pr)))
       (if (%lru-cache-deleter c)
           ((%lru-cache-deleter c) k (%node-value n))
           #t))))

  (define (lru-cache-fold c kons knil)
    (let loop ((x (lru-cache-head c))
               (xs knil))
      (if (not x)
          xs
          (loop (lru-node-next x)
                (kons (lru-node-key x) (lru-node-value x) xs)))))

  ;; Call (proc k v) for each key, value in the cache.  Nodes are
  ;; traversed from MRU to LRU.
  (define (lru-cache-walk c proc)
    (do ((n (lru-cache-head c) (lru-node-next n)))
        ((not n))
      (proc (lru-node-key n) (lru-node-value n))))

  ;; Delete all nodes in the cache C. The deleter (if provided) is run
  ;; for each node as the node list is traversed from head to tail.  If
  ;; an error occurs in the deleter, the offending node will be left at
  ;; the head of the cache.
  (define (lru-cache-flush! c)
    (let ((del (lru-cache-deleter c))
          (ht (lru-cache-ht c)))
      (do ((n (lru-cache-head c) (lru-node-next n)))
          ((not n))
        (lru-cache-head-set! c n)
        (and del
             (del (lru-node-key n) (lru-node-value n)))
        (hash-table-delete! ht (lru-node-key n)))
      (lru-cache-head-set! c #f)
      (lru-cache-tail-set! c #f)))

)

