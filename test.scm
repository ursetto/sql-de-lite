(use test)
(use sqlite3-simple)

;; Concatenate string literals into a single literal at compile time.
;; (string-literal "a" "b" "c") -> "abc"
(define-syntax string-literal
  (lambda (f r c)
    (apply string-append (cdr f))))

#|

(begin
  (define stmt (prepare db "create table cache(key text primary key, val text);"))
  (step stmt)
  (step (prepare db "insert into cache values('ostrich', 'bird');"))
  (step (prepare db "insert into cache values('orangutan', 'monkey');"))
)

(use sqlite3-simple)
(raise-database-errors #t)
(define db (open-database "a.db"))
(define stmt2 (prepare db "select rowid, key, val from cache;"))
(step stmt2)
(column-count stmt2)  ; => 3
(column-type stmt2 0) ; => integer
(column-type stmt2 1) ; => text
(column-type stmt2 2) ; => text
(column-data stmt2 0) ; => 1
(column-data stmt2 1) ; => "ostrich"
(column-data stmt2 2) ; => "orangutan"
(row-data stmt2)
(row-alist stmt2)
(define stmt3 (prepare db "select rowid, key, val from cache where key = ?;"))
(fetch (bind (reset stmt3) 1 "orangutan"))
(fetch (bind (reset stmt3) 1 (string->blob "orangutan"))) ; fails.  dunno why
(step (bind (prepare db "insert into cache values(?, 'z');")
            1 (string->blob "orange2")))
(blob->string (alist-ref 'key (fetch-alist (bind (reset stmt3) 1 (string->blob "orange2"))))) ; -> "orange2"
(fetch stmt3)
(define stmt4 (prepare db "select rowid, key, val from cache where rowid = ?;"))
(fetch (bind (reset stmt4) 1 2))

(call-with-database "a.db" (lambda (db) (fetch (prepare db "select * from cache;"))))
  ; -> ("ostrich" "bird")  + finalization warning

(call-with-database "a.db" (lambda (db) (call-with-prepared-statements db (list "select * from cache;" "select rowid, key, value from cache;") (lambda (s1 s2) (and s1 s2 (list (fetch s1) (fetch s2)))))))   ; #f (or error) -- invalid column name
(call-with-database "a.db" (lambda (db) (call-with-prepared-statements db (list "select * from cache;" "select rowid, key, val from cache;") (lambda (s1 s2) (and s1 s2 (list (fetch s1) (fetch s2)))))))     ; (("ostrich" "bird") (1 "ostrich" "bird"))


(call-with-database ":memory:" (lambda (db) (with-transaction db (lambda () (fetch (execute-sql db "select 1 union select 2")) (error 'oops))))) ; => same as above but error is thrown
(call-with-database ":memory:" (lambda (db) (with-transaction db (lambda () (call-with-prepared-statement db "select 1 union select 2" (lambda (s) (fetch (execute s)) (error 'oops))) #f))))   ; => error, same as previous
(call-with-database ":memory:" (lambda (db) (with-transaction db (lambda () (call-with-prepared-statement db "select 1 union select 2" (lambda (s) (fetch (execute s)))) #f))))   ; => #f, statement finalized by call-with-prepared-statement

|#

(raise-database-errors #t)

(test-group
 "autocommit"
 (test "autocommit? reports #t outside transaction" #t
       (call-with-database ":memory:"
         (lambda (db)
           (autocommit? db))))
 (test "autocommit? reports #f during transaction" #f
       (call-with-database ":memory:"
         (lambda (db)
           (with-transaction db
             (lambda ()
               (autocommit? db)))))))

(test-group
 "rollback"
 (test-error "Open queries prevent SQL ROLLBACK"
       (call-with-database ":memory:"
         (lambda (db)
           (execute-sql db "begin;")
           (or (equal? '(1) (fetch (execute-sql db "select 1 union select 2")))
               (error 'fetch "fetch failed during test"))
           (execute-sql db "rollback;"))))  ; will throw SQLITE_BUSY
 (test "(rollback) resets open queries" 0
       ;; We assume reset worked if no error; should we explicitly test it?
       (call-with-database ":memory:"
         (lambda (db)
           (execute-sql db "begin;")
           (or (equal? '(1) (fetch (execute-sql db "select 1 union select 2")))
               (error 'fetch "fetch failed during test"))
           (rollback db))))
 (test "with-transaction rollback resets open queries" #f
       ;; We assume reset worked if no error; should we explicitly test it?
       (call-with-database ":memory:"
         (lambda (db)
           (with-transaction db
             (lambda ()
               (or (equal? '(1)
                           (fetch (execute-sql db "select 1 union select 2")))
                   (error 'fetch "fetch failed during test"))
               #f ; rollback
               ))))))

(test-group
 "reset"
 (test "with-query resets statement immediately (normal exit)"
       '((1) (1))
       (call-with-database ":memory:"
         (lambda (db)
           (let-prepare db ((s "select 1 union select 2;"))
             (list (with-query s (lambda () (fetch s)))
                   (with-query s (lambda () (fetch s))))))))
 (test "with-query resets statement immediately (error exit)"
       '((oops) (1))
       (call-with-database ":memory:"
         (lambda (db)
           (let-prepare db ((s "select 1 union select 2;"))
             (list (handle-exceptions exn '(oops)
                     (with-query s (lambda () (fetch s) (error 'oops))))
                   (with-query s (lambda () (fetch s)))))))))

;; syntax wrong -- query body cannot access statement
;; (test "query/fetch first-row"
;;       '(1 2)
;;       (call-with-database ":memory:"
;;         (lambda (db)
;;           (query (execute-sql db "select 1, 2 union select 3, 4;")
;;                  (fetch s)))))

;; (test "query/fetch first row via (query s (fetch s))"
;;       '(1 2)
;;       (call-with-database ":memory:"
;;         (lambda (db)
;;           (let-prepare db ((s "select 1, 2 union select 3, 4;"))
;;             (query s (fetch s))))))

;; (test "query/fetch first row via (query fetch)"
;;       '(1 2)
;;       (call-with-database ":memory:"
;;         (lambda (db)
;;           (let ((q (query db "select 1, 2 union select 3, 4;" fetch)))
;;             (q)))))

(test "query/fetch first row via first-row"
      '(1 2)
      (call-with-database ":memory:"
        (lambda (db)
          (let-prepare db ((s "select 1, 2 union select 3, 4;"))
            (first-row s)))))

(test "query/fetch all rows via fetch-all"
      '(((1 2) (3 4) (5 6)) reset ((1 2) (3 4) (5 6)))
      (call-with-database ":memory:"
        (lambda (db)
          (let-prepare db ((s (string-literal "select 1, 2 union "
                                              "select 3, 4 union "
                                              "select 5, 6;")))
            (list (fetch-all s)
                  'reset
                  (fetch-all s))))))

(test "fetch-all reads remaining rows mid-query"
      '((1 2) fetch ((3 4) (5 6)))
      (call-with-database ":memory:"
        (lambda (db)
          (let-prepare db ((s (string-literal "select 1, 2 union "
                                              "select 3, 4 union "
                                              "select 5, 6;")))
            (list (fetch s)
                  'fetch
                  (fetch-all s))))))

(test "Pending open queries are finalized after let-prepare"
      '((1) (3))
      (let ((db (open-database ":memory:")))
        (let ((rv 
               (let-prepare db ((s1 "select 1 union select 2")
                                (s2 "select 3 union select 4"))
                 (list (fetch (execute s1)) (fetch (execute s2))))))
          (close-database db)
          rv)))

(test "Pending open queries are finalized when DB is closed"
      '((1) (3))
      (let* ((db (open-database ":memory:"))
             (s1 (prepare db "select 1 union select 2"))
             (s2 (prepare db "select 3 union select 4")))
        (let ((rv (list (fetch (execute s1)) (fetch (execute s2)))))
          (close-database db) ; finalize here
          rv)))

;; let-prepare finalization will error when database is closed
;; Should actually succeed, as ideally statements will be set to #f
;; upon database close.
(test "Finalizing previously finalized statement OK even after DB is closed"
      '((1) (3))
      (let ((db (open-database ":memory:")))
        (let-prepare db ((s1 "select 1 union select 2")
                         (s2 "select 3 union select 4"))
          (let ((rv (list (fetch (execute s1)) (fetch (execute s2)))))
            (close-database db)
            rv))))   ; s1 and s2 are refinalized after let-prepare

(test-error "Pending statements are finalized on error in call-with-database"
            ;; Should receive error 'oops here
            ;; FIXME should test that statements are actually finalized
            ;; rather than relying on error 'oops reception and manual
            ;; inspection of warning.  Also verify finalization occurs in
            ;; call-with-database, not let-prepare
      (call-with-database ":memory:"
        (lambda (db)
          (let-prepare db ((s1 "select 1 union select 2")
                           (s2 "select 3 union select 4"))
            (list (fetch (execute s1))
                  (error 'oops)
                  (fetch (execute s2)))))))

(test "create / insert one row via execute-sql"
      1
      (call-with-database ":memory:"
        (lambda (db)
          (execute-sql db "create table cache(k,v);")
          (execute-sql db "insert into cache values('jml', 'oak');"))))

(test-error   ;; operation on finalized statement
 "Execute after finalize fails"
 (call-with-database ":memory:"
   (lambda (db)
     (execute-sql db "create table cache(k,v);")
     (let-prepare db ((s "insert into cache values('jml', 'oak');"))
       (finalize s)
       (execute s)))))

(test-error   ;;  operation on closed database
 ;; Expected: Warning: finalizing pending statement: "insert into cache values('jml', 'oak');"
 "Operating on statement fails after database close"
 (let ((s (call-with-database ":memory:"
            (lambda (db)
              (execute-sql db "create table cache(k,v);")
              (prepare db "insert into cache values('jml', 'oak');")))))
   (execute s))) 

(test "Double finalization ignored in let-prepare + execute"
      1
      (call-with-database ":memory:"
        (lambda (db)
          (execute-sql db "create table cache(k,v);")
          (let-prepare db ((s "insert into cache values('jml', 'oak');"))
            (execute s)))))

(test "Successful rollback outside transaction"
      #t
      (call-with-database ":memory:"
        (lambda (db) (rollback db))))

(test "Successful commit outside transaction"
      #t
      (call-with-database ":memory:"
        (lambda (db) (commit db))))

(test "insert ... select executes in one step"
      '((3 4) (5 6))
      (call-with-database ":memory:"
        (lambda (db)
          (define (e x) (exec (sql db x)))
          (e "create table a(k,v);")
          (e "create table b(k,v);")
          (e "insert into a values(3,4);")
          (e "insert into a values(5,6);")
          (step (prepare db "insert into b select * from a;")) ; the test
          (query fetch-all (sql db "select * from b;")))))

(test "cached statement may be exec'ed multiple times"
      0
      (call-with-database ":memory:"
          (lambda (db)
            (exec (sql db "create table a(k primary key, v);"))
            (exec (sql db "insert into a values(?,?)")
                  "foo" "bar")
            (exec (sql db "insert or ignore into a values(?,?)")
                  "foo" "bar")
            (exec (sql db "insert or ignore into a values(?,?)")
                  "foo" "bar"))))

(test-error "invalid bound parameter type (procedure) throws error"
            (call-with-database 'memory
              (lambda (db)
                (exec (sql db "create table a(k,v);"))
                (exec (sql db "select * from a where k = ?;")
                      identity))))

(test-group
 "open-database"
 (test "open in-memory database using 'memory"
       '(1 2)
       (call-with-database 'memory
         (lambda (db) (exec (sql db "select 1,2;")))))
 (test "open temp database using 'temp"
       '(1 2)
       (call-with-database 'temp
         (lambda (db) (exec (sql db "select 1,2;")))))
 (test "open temp database using 'temporary"
       '(1 2)
       (call-with-database 'temporary
         (lambda (db) (exec (sql db "select 1,2;")))))
 ;;(test "home directory expansion")
 )

(test-group
 "large integers"
 ;; note int64 range on 32-bit is -2^53 ~ 2^53-1 where 2^53=9007199254740992
 (call-with-database ":memory:"
   (lambda (db)
     (let ((rowid 1234567890125))
       (exec (sql db "create table cache(k,v);"))
       ;; Note the hardcoded insert to ensure the value is correct.
       (exec (sql db "insert into cache(rowid,k,v) values(1234567890125, 'jimmy', 'dunno');"))
       (test (conc "last-insert-rowid on int64 rowid " rowid)
             rowid
             (last-insert-rowid db))
       (test (conc "retrieve row containing int64 rowid " rowid)
             `(,rowid "jimmy" "dunno")
             (exec (sql db "select rowid, * from cache where rowid = ?;")
                   rowid))))))

;;; Future tests

;; ;; test result: reset should fail with 'operation on finalized statement'
;; (use posix)
;; (call-with-database "a.db"
;;   (lambda (db)
;;     (let-prepare db ((s "select * from cache;"))
;;       (set! *s1* s)
;;       (sleep 10)        ; database must get locked exclusive elsewhere now
;;       (parameterize ((raise-database-errors #f))
;;         (and (step s) (error "step should have failed due to lock"))))
;;     ;; Statement should successfully be finalized in let-prepare
;;     (reset *s1*)))    ;; reset should fail with finalized statement error

(test-exit)
