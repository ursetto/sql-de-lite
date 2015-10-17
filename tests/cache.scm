
(use test sql-de-lite-cache)

(test-group "cache"
            
(test-group
 "add/remove"
 (define DL '())
 (define C (make-lru-cache 3 string=?
                           (lambda (k v) (set! DL (cons (cons k v) DL)))))
 (test "add"
       '(3 ("foo" . 10))
       (begin
         (lru-cache-add! C "foo" 10)
         (lru-cache-add! C "bar" 11)
         (lru-cache-add! C "baz" 12)
         (lru-cache-add! C "qux" 13)
         (cons (lru-cache-size C)
               DL)))
; bar baz qux
 (test "remove"
       '(("baz" . 12) #f ("bar" . 11) ("qux" . 13) 0 (("foo" . 10)))
       (list (lru-cache-remove! C "baz")
             (lru-cache-remove! C "poo")
             (lru-cache-remove! C "bar")
             (lru-cache-remove! C "qux")      
             (lru-cache-size C)
             DL)))

(test-group
 "flush"
 (define DL '())
 (define C (make-lru-cache 3 string=?
                           (lambda (k v) (set! DL (cons (cons k v) DL)))))
 (test "add"
       '(3 ("foo" . 10))
       (begin
         (lru-cache-add! C "foo" 10)
         (lru-cache-add! C "bar" 11)
         (lru-cache-add! C "baz" 12)
         (lru-cache-add! C "qux" 13)
         (cons (lru-cache-size C)
               DL)))
 (test "flush"
       ;; assumes flushed in LRU->MRU order; but order not defined
       '(0 #f
           (("qux" . 13) ("baz" . 12) ("bar" . 11) ("foo" . 10)))
       (begin
         (lru-cache-flush! C)
         (list (lru-cache-size C)
               (lru-cache-remove! C "bar")
               DL))))

(test-group
 "multi"
 (define DL '())
 (define C (make-lru-cache 3 string=?
                           (lambda (k v) (set! DL (cons (cons k v) DL)))))
 (test "add"
       '(3 ("foo" . 10))
       (begin
         (lru-cache-add! C "foo" 10)
         (lru-cache-add! C "foo" 11)
         (lru-cache-add! C "foo" 12)
         (lru-cache-add! C "foo" 13)
         (cons (lru-cache-size C)
               DL)))
 ;; Assumes removal in MRU->LRU order.  This makes sense as we want the MRU when removing an entry to reuse,
 ;; but behavior is not guaranteed.
 (test "remove"
       '(("foo" . 13) ("foo" . 12) ("foo" . 11) 0 (("foo" . 10)))
       (list (lru-cache-remove! C "foo")
             (lru-cache-remove! C "foo")
             (lru-cache-remove! C "foo")
             (lru-cache-size C)
             DL))
 
)
)
