(use test)
(use sqlite3-simple)

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

;; test large numbers; note 2^53=9007199254740992   -2^53 ~ 2^53-1 
(step (prepare db "insert into cache(rowid,key,val) values(1234567890125, 'jimmy', 'dunno');")) ;=>1
(last-insert-rowid db) => 1234567890125.0
(fetch (bind (prepare db "select rowid, * from cache where rowid = ?;") 1 1234567890125.0))  ; => (1234567890125.0 "jimmy" "dunno")
(execute-sql db "insert into cache(rowid,key,val) values(4294967295, 'moby', 'whale');")
(fetch (execute-sql db "select rowid, * from cache where rowid = ?;" 4294967295))  ; => (4294967295.0 "moby" "whale")


(call-with-database ":memory:" (lambda (db) (with-transaction db (lambda () (fetch (execute-sql db "select 1 union select 2")) #f))))  ; => #f, plus statement will be reset by rollback and finalized by call/db
(call-with-database ":memory:" (lambda (db) (with-transaction db (lambda () (fetch (execute-sql db "select 1 union select 2")) (error 'oops))))) ; => same as above but error is thrown
(call-with-database ":memory:" (lambda (db) (with-transaction db (lambda () (call-with-prepared-statement db "select 1 union select 2" (lambda (s) (fetch (execute s)) (error 'oops))) #f))))   ; => error, same as previous
(call-with-database ":memory:" (lambda (db) (with-transaction db (lambda () (call-with-prepared-statement db "select 1 union select 2" (lambda (s) (fetch (execute s)))) #f))))   ; => #f, statement finalized by call-with-prepared-statement

|#

(raise-database-errors #t)

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
(test "Finalizing previously finalized statement succeeds after DB is closed"
      '((1) (3))
      (let ((db (open-database ":memory:")))
        (let-prepare db ((s1 "select 1 union select 2")
                         (s2 "select 3 union select 4"))
          (let ((rv (list (fetch (execute s1)) (fetch (execute s2)))))
            (close-database db)
            (error 'hi)
            rv))))

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

