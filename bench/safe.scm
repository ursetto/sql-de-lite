(use sql-de-lite)
(use miscmacros)
(define db (open-database 'memory))

(print "Creating table")
(exec (sql db "create table ht(v integer);"))
(let ((s (sql db "insert into ht(v) values(1);")))
  (time
   (dotimes (i 1000000) (exec s))))

(define *count* 1000000)

;; (print
;;  (time
;;   (query (fold-rows (query (fold-rows (lambda (x xs) (+ (car x) xs))
;;                                       0)))
;;          (sql db "select 1 from ht;"))))


(print "Step " *count* " times, no callbacks registered")

(let ((s (prepare-transient db "select 1 from ht;")))
  (time
   (dotimes (i *count*) (step s))))

(print "Step " *count* " times, built-in function, no callbacks registered")

(let ((s (prepare-transient db "select random() from ht;")))
  (time
   (dotimes (i *count*) (step s))))

(register-scalar-function! db "foo" 1 (lambda (x) 1))

(print "Step " *count* " times, no callback, callback registered")

(let ((s (prepare-transient db "select 1 from ht;")))
  (time
   (dotimes (i *count*) (step s))))

(print "Step " *count* " times, built-in function, callback registered")

(let ((s (prepare-transient db "select random() from ht;")))
  (time
   (dotimes (i *count*) (step s))))

(print "Step " *count* " times, user callback, callback registered")

(let ((s (prepare-transient db "select foo(v) from ht;")))
  (time
   (dotimes (i *count*) (step s))))




#|

Example bench:

(csc -O5)

All steps safe

 Creating table
 8.491s CPU time, 1.298s GC time (major), 1046404 mutations, 5847/994153 GCs (major/minor)
 Step 1000000 times, no callbacks registered
 2.578s CPU time, 0.989s GC time (major), 1008000 mutations, 4441/995559 GCs (major/minor)
 Step 1000000 times, built-in function, no callbacks registered
 2.761s CPU time, 0.972s GC time (major), 1008000 mutations, 4441/995559 GCs (major/minor)
 Step 1000000 times, no callback, callback registered
 2.58s CPU time, 0.944s GC time (major), 1008000 mutations, 4445/995555 GCs (major/minor)
 Step 1000000 times, built-in function, callback registered
 2.76s CPU time, 0.949s GC time (major), 1008000 mutations, 4445/995555 GCs (major/minor)
 Step 1000000 times, user callback, callback registered
 5.637s CPU time, 1.088s GC time (major), 4000000 mutations, 5000/1995000 GCs (major/minor)

Steps unsafe until any callback registered, then all steps safe (current implementation)

 Creating table
 5.118s CPU time, 0.005s GC time (major), 44804 mutations, 29/5518 GCs (major/minor)
 Step 1000000 times, no callbacks registered
 0.275s CPU time, 6400 mutations, 5/1308 GCs (major/minor)
 Step 1000000 times, built-in function, no callbacks registered
 0.446s CPU time, 0.002s GC time (major), 6400 mutations, 5/1308 GCs (major/minor)
 Step 1000000 times, no callback, callback registered
 2.612s CPU time, 0.984s GC time (major), 1008000 mutations, 4588/995412 GCs (major/minor)
 Step 1000000 times, built-in function, callback registered
 2.846s CPU time, 0.989s GC time (major), 1008000 mutations, 4589/995411 GCs (major/minor)
 Step 1000000 times, user callback, callback registered
 5.689s CPU time, 1.13s GC time (major), 4000000 mutations, 5154/1994846 GCs (major/minor)

|#
