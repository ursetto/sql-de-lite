(cond-expand
  (chicken-4
   (use test)
   (use sql-de-lite)
   (use compile-file)
   (use files)                          ; create-temporary-file
   (use posix))                         ; delete-file
  (else
   (import test sql-de-lite compile-file (chicken file) (chicken blob)
           (chicken pathname) (chicken string) (chicken format))))

;; Concatenate string literals into a single literal at compile time.
;; (string-literal "a" "b" "c") -> "abc"
(define-syntax string-literal
  (er-macro-transformer
   (lambda (f r c)
     (apply string-append (cdr f)))))
(define-syntax begin0                 ; multiple values discarded
  (syntax-rules () ((_ e0 e1 ...)
                    (let ((tmp e0)) e1 ... tmp))))

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
 (test "autocommit? reports #t outside transaction" '(#t)
       (call-with-database ":memory:"
         (lambda (db)
           (list (autocommit? db)))))
 (test "autocommit? reports #f during transaction" '(#f)   ; box in list because #f will rollback transaction
       (call-with-database ":memory:"
         (lambda (db)
           (with-transaction db
             (lambda ()
               (list (autocommit? db))))))))

(test-group
 "rollback"

 (test "Open read queries do not prevent SQL ROLLBACK"  ; behavior changed: allowed in >= 3.7.11
       0
       (call-with-database ":memory:"
                           (lambda (db)
                             (exec (sql db "begin;"))
                             (or (equal? '(1)
                                         (fetch (prepare db "select 1 union select 2")))
                                 (error 'fetch "fetch failed during test"))
                             (exec (sql db "rollback;")))))
 (test "Open read queries ok with SQL COMMIT"
       #t
       (call-with-database ":memory:"
         (lambda (db)
           (exec (sql db "begin;"))
           (or (equal? '(1)
                       (fetch (prepare db "select 1 union select 2")))
               (error 'fetch "fetch failed during test"))
           (exec (sql db "commit;"))
           #t)))
 (test "(rollback) resets open cached queries"
       0
       ;; We assume reset worked if no error; should we explicitly test it?
       (call-with-database ":memory:"
         (lambda (db)
           (exec (sql db "begin;"))
           (or (equal? '(1)
                       (fetch (prepare db "select 1 union select 2")))
               (error 'fetch "fetch failed during test"))
           (rollback db))))
 (test "(rollback) resets open transient queries (and warns)"
       0
       (call-with-database ":memory:"
         (lambda (db)
           (exec (sql db "begin;"))
           (or (equal? '(1)
                       (fetch (prepare-transient db
                                                 "select 1 union select 2")))
               (error 'fetch "fetch failed during test"))
           (rollback db))))
 (test "with-transaction rollback resets open queries"
       #f
       ;; We assume reset worked if no error; should we explicitly test it?
       (call-with-database ":memory:"
         (lambda (db)
           (with-transaction db
             (lambda ()
               (or (equal? '(1)
                           (fetch (prepare db "select 1 union select 2")))
                   (error 'fetch "fetch failed during test"))
               #f ; rollback
               ))))))

;; FIXME: It is not possible to test statements for reset status.  We could test for finalized status though.

(test-group
 "reset"
 (test "fetch after query is illegal (normal exit)"      ; behavior now illegal -- resurrect required
       '((1) (fetch-exn))
       (call-with-database ":memory:"
         (lambda (db)
           (let ((s (sql db "select 1 union select 2;")))
             (list (query fetch s)
                   (handle-exceptions exn '(fetch-exn)
                     (fetch s)))))))
 (test "fetch after query is illegal (error exit)"
       '((query-exn) (fetch-exn))
       (call-with-database ":memory:"
         (lambda (db)
           (let ((s (sql db "select 1 union select 2;")))
             (list (handle-exceptions exn '(query-exn)
                     (query (lambda (s) (fetch s) (error 'query-exn))
                            s))
                   (handle-exceptions exn '(fetch-exn)
                     (fetch s)))))))
 (test "fetch after exec is illegal"  ; behavior now illegal
       '((1) (fetch-exn))
       (call-with-database ":memory:"
         (lambda (db)
           (let ((s (sql db "select 1 union select 2;")))
             (list (exec s)
                   (handle-exceptions exn '(fetch-exn)
                     (fetch s)))))))
 (test "query finalizes statement -- cache on"      ; really we just want to test for reset
       '((1) #t)
       (call-with-database ":memory:"
         (lambda (db)
           (let ((s (sql db "select 1 union select 2;")))
             (list (query fetch s)
                   (finalized? s))))))
 (test "query finalizes statement -- cache off"      ; really we just want to test for reset
       '((1) #t)
       (parameterize ((prepared-cache-size 0))
         (call-with-database
          ":memory:"
          (lambda (db)
            (let ((s (sql db "select 1 union select 2;")))
              (list (query fetch s)
                    (finalized? s))))))) 
 (test "exec finalizes statement -- cache on"      ; really we just want to test for reset
       '((1) #t)
       (call-with-database ":memory:"
         (lambda (db)
           (let ((s (sql db "select 1 union select 2;")))
             (list (exec s)
                   (finalized? s))))))
 (test "exec finalizes statement -- cache off"
       '((1) #t)
       (parameterize ((prepared-cache-size 0))
         (call-with-database
          ":memory:"
          (lambda (db)
            (let ((s (sql db "select 1 union select 2;")))
              (list (exec s)
                    (finalized? s)))))))
 (test "exec resets even when column count = 0"
       ;; We now unconditionally reset after any exec, even if it returns
       ;; no rows, because ROLLBACK does not behave properly if not reset.
       '(1 #t)
             (call-with-database ":memory:"
               (lambda (db)
                 (exec (sql db "create table a(k,v);"))
                 (let ((s (sql db "insert or ignore into a values(1,2);")))
                   (list (exec s)
                         (finalized? s))))))
 )

(test-group
 "fetch"

(test "fetch first row via fetch"
      '(1 2)
      (call-with-database ":memory:"
        (lambda (db)
          (let ((s (prepare db "select 1, 2 union select 3, 4;")))
            (fetch s)))))

(test "fetch first row via exec"
      '(1 2)
      (call-with-database ":memory:"
        (lambda (db)
          (let ((s (sql db "select 1, 2 union select 3, 4;")))
            (exec s)))))

(test "fetch first row via (query fetch ...)"
      '(1 2)
      (call-with-database ":memory:"
        (lambda (db)
          (let ((s (sql db "select 1, 2 union select 3, 4;")))
            (query fetch s)))))

(test "fetch all rows twice via fetch-all + reset + fetch-all"
      '(((1 2) (3 4) (5 6)) reset ((1 2) (3 4) (5 6)))
      (call-with-database ":memory:"
        (lambda (db)
          (let ((s (prepare db (string-literal "select 1, 2 union "
                                               "select 3, 4 union "
                                               "select 5, 6;"))))
            (list (fetch-all s)
                  (begin (reset s) 'reset)
                  (fetch-all s))))))

;; Test disabled because it amounts to misuse of interface.  Although
;; autoreset was added in 3.6.23.2 (and made configurable in 3.7.5
;; via SQLITE_OMIT_AUTORESET) it was only intended to fix broken applications.
;; And it seems the version of sqlite included with OS X Lion (3.7.5) is
;; built with autoreset omitted, so we can't rely on consistent behavior
;; based on version.  Therefore, we declare this behavior undefined.
#;
(test "fetch all rows twice via fetch-all + fetch-all (requires >= 3.6.23.2)"
      '(((1 2) (3 4) (5 6)) library-reset ((1 2) (3 4) (5 6)))
      (call-with-database ":memory:"
        (lambda (db)
          (let ((s (prepare db (string-literal "select 1, 2 union "
                                               "select 3, 4 union "
                                               "select 5, 6;"))))
            (list (fetch-all s)
                  'library-reset
                  (fetch-all s))))))

(test "fetch all rows twice via (query fetch-all ...) x 2"
      '(((1 2) (3 4) (5 6)) ((1 2) (3 4) (5 6)))
      (call-with-database ":memory:"
        (lambda (db)
          (let ((s (prepare db (string-literal "select 1, 2 union "
                                               "select 3, 4 union "
                                               "select 5, 6;"))))
            (list (query fetch-all s)
                  ; reset not required
                  (query fetch-all s))))))

(test "fetch-all reads remaining rows mid-query"
      '((1 2) fetch ((3 4) (5 6)))
      (call-with-database ":memory:"
        (lambda (db)
          (let ((s (prepare db (string-literal "select 1, 2 union "
                                               "select 3, 4 union "
                                               "select 5, 6;"))))
            (list (fetch s)
                  'fetch
                  (fetch-all s))))))
)

;; No way to really test this other than inspecting warnings
;; (test "Pending cached queries are finalized when DB is closed"
;;       '((1) (3))
;;       (let* ((db (open-database ":memory:"))
;;              (s1 (prepare db "select 1 union select 2"))
;;              (s2 (prepare db "select 3 union select 4")))
;;         (let ((rv (list (fetch s1) (fetch s2))))
;;           (close-database db) ; finalize here
;;           rv)))

;; let-prepare finalization will error when database is closed
;; Should actually succeed, as ideally statements will be set to #f
;; upon database close.
(test "Finalizing previously finalized statement OK even after DB is closed"
      '((1) (3))
      (let ((db (open-database ":memory:")))
        (let ((s1 (prepare-transient db "select 1 union select 2"))
              (s2 (prepare-transient db "select 3 union select 4")))
          (let ((rv (list (fetch s1)
                          (fetch s2))))
            (close-database db)
            (finalize s1)
            (finalize s2)
            rv))))

(test-error "step after db closed raises exception"
            (let ((db (open-database ":memory:")))
              (let ((s1 (prepare-transient db "select 1;")))
                (and (eqv? 'row (step s1))
                     (eqv? 'done (step s1))
                     (finalize s1)
                     (close-database db)      ;; returns #f if statements still open
                     (step s1)))))

(test "Open transient statements closed after call/db"
      ;; They are finalized (you may receive a warning), but don't show
      ;; up as FINALIZED?.  Currently, we do not confirm finalization
      ;; other than through manually inspecting the warning.
      #t
      (let ((s1 #f) (s2 #f) (db0 #f))
        (handle-exceptions ex
            (and (finalized? s1) (finalized? s2)
                 (database-closed? db0))
          (call-with-database ":memory:"
            (lambda (db)
              (set! db0 db)
              (set! s1 (prepare-transient db "select 1 union select 2"))
              (set! s2 (prepare-transient db "select 3 union select 4"))
              (step s1)
              (error 'oops))))))
(test "Prepared stmts FINALIZED? after QUERY bind error (cache disabled)"
      ;; Ensure QUERY finalizes uncached statements in case of bind error.
      #t
      (let ((s1 #f) (db0 #f))
        (handle-exceptions ex
            (and (finalized? s1) (database-closed? db0))
          (parameterize ((prepared-cache-size 0))
            (call-with-database
             ":memory:"
             (lambda (db)
               (set! db0 db)
               (set! s1 (sql db "select * from sqlite_master where sql=?"))
               ;; Generate a bind error (#f is invalid argument type)
               (query fetch s1 #f)
               #t))))))
(test "Prepared stmts FINALIZED? after EXEC bind error (cache disabled)"
      ;; Ensure EXEC finalizes uncached statements in case of bind error.
      ;; Copy and paste from QUERY version.
      #t
      (let ((s1 #f) (db0 #f))
        (handle-exceptions ex
            (and (finalized? s1) (database-closed? db0))
          (parameterize ((prepared-cache-size 0))
            (call-with-database
             ":memory:"
             (lambda (db)
               (set! db0 db)
               (set! s1 (sql db "select * from sqlite_master where sql=?"))
               ;; Generate a bind error (#f is invalid argument type)
               (exec s1 #f)
               #t))))))
#;
(test "Prepared statements are FINALIZED? after QUERY bind error (expired)"
      ;; Ensure QUERY finalizes statements expired from cache in case of bind error.
      ;; This is a roundabout way of testing whether the cached flag is removed
      ;; from cached statements after they expire.  Incomplete implementation -- doesn't
      ;; look like this bug can be triggered, because the statement can have four
      ;; states: 1) cached flag with ptr non-null (resurrected/prepared statement in cache);
      ;;         2) cached flag with ptr null (finalized statement expired from cache);
      ;;         3) no cached flag with ptr non-null (transient prepared statement);
      ;;         4) no cached flag with ptr null  (transient finalized statement).
      ;; and cached flag is only tested in two situations:
      ;;         1) directly after resurrect in QUERY/EXEC, so the statement must be in state #1 or #3
      ;;         2) in finalize, but statement must also have ptr non-null to pass finalize-transient,
      ;;            so must be in state #1 or #3
      ;; FIXME: We test QUERY here but not EXEC.
      #t
      (let ((s1 #f) (db0 #f))
        (handle-exceptions ex
            (and (finalized? s1) (database-closed? db0))
          (parameterize ((prepared-cache-size 1))
            (call-with-database
             ":memory:"
             (lambda (db)
               (set! db0 db)
               ;; Generate a bind error (symbol is invalid argument type)
               (set! s1 (sql db "select * from sqlite_master where sql=?"))
               (query fetch s1 'foo)
               #t))))))

(call-with-database
 ":memory:"
 (lambda (db)
   (let ((s (sql/transient db "select 1 union select 2")))
     (test-assert "sql/transient statements are initially finalized"
                  (finalized? s))
     ;; This is functionally the same as the (sql ...) example above with cache-size 0,
     ;; but we want to be thorough.
     (test-assert "sql/transient statements are finalized after QUERY error"
                  (begin
                    (handle-exceptions e #f
                      (query (lambda (s) (step s) (error))
                             s))
                    (finalized? s))))))


(test "Cached statements are finalized on error in call-with-database"
      #t
      (let ((s1 #f) (s2 #f) (db0 #f))
        (handle-exceptions ex
            (and (finalized? s1) (finalized? s2) (database-closed? db0))
          (call-with-database ":memory:"
            (lambda (db)
              (set! db0 db)
              (set! s1 (prepare db "select 1 union select 2"))
              (set! s2 (prepare db "select 3 union select 4"))
              (step s1)
              (error 'oops))))))

(test-error "query* on non-statement does not enter infinite loop"
            ;; Check that we don't raise exception in fast-unwind-protect*.
            ;; This test can never fail! ;)
            (call-with-database ":memory:" (lambda (db) (query* #f #f))))

(test "Reset cached statement may be pulled from cache"
      #t   ; Cannot currently dig into statement to test it; just ensure no error
      (call-with-database 'memory
        (lambda (db)
          (let* ((sql "select 1;")
                 (s1 (prepare db sql))
                 (s2 (prepare db sql)))
            #t))))

(test "Identical (text-wise) running nested SQL statements are allowed"
      ;; This requires either that we prepare a separate statement if a cached statement is running,
      ;; or that statements are only cached while they are not running (better).
      '(1 . 2)
      (call-with-database
       'memory
       (lambda (db)
         (query (lambda (s)
                  (let* ((v (fetch-value s))
                         (v2 (query fetch-value (sql db "select ?;") 2)))
                    (cons v v2)))
                (sql db "select ?;") 1))))

;; The below results in an infinite loop of (1) (3) (3) ... . The inner query will rebind the parameters to 3,4
;; (which will thus be seen later by the outer loop), and it also resets the query on termination, so the outer
;; loop starts over from the beginning (now 3).
;; FIXME: We may need to make (resurrect) error out if the statement is running.  This would avoid the infinite loop
;; (which is a programming error) and, since we will transition to a cache that does not hold running statements,
;; is not needed there.
#;
(test "Identical (object-wise) running nested SQL statements are allowed; inner resets outer"
      '(1 . 2)
      (call-with-database
       'memory
       (lambda (db)
         (let ((s (sql db "select ? union select ?;")))
           (query (map-rows (lambda (row)
                              (print row)
                              (append row
                                      (query fetch-value s 3 4))))
                  s 1 2)))))

(test "create / insert one row via execute-sql"
      1
      (call-with-database ":memory:"
        (lambda (db)
          (exec (sql db "create table cache(k,v);"))
          (exec (sql db "insert into cache values('jml', 'oak');")))))

(define (check-error-status code thunk)
  (condition-case (begin (thunk) #f)
   (e (exn sqlite) (eqv? code (sqlite-exception-status e)))))

(call-with-database
 ":memory:"
 (lambda (db)
   (test-assert
    "empty SQL is illegal"
    (check-error-status 'ok (lambda () (prepare db ""))))   
   (test-assert
    "whitespace SQL is illegal"
    (check-error-status 'ok (lambda () (prepare db " "))))
   (test-assert
    "comment-only SQL is illegal"
    (check-error-status 'ok (lambda () (prepare db "-- comment"))))))

(test-group
 "finalization"
 (test ;; operation on finalized statement
  "exec after finalize succeeds (statement resurrected)"
  1
  (call-with-database ":memory:"
    (lambda (db)
      (exec (sql db "create table cache(k,v);"))
      (let ((s (prepare-transient
                db "insert into cache values('jml', 'oak');")))
        (finalize s)
        (exec s)))))
 (test
  "reset after finalize ok"
  #t
  (call-with-database ":memory:"
    (lambda (db)
      (exec (sql db "create table cache(k,v);"))
      (let ((s (prepare-transient
                db "insert into cache values('jml', 'oak');")))
        (finalize s)
        (reset s)
        #t)))) 

 (test-error ;;  operation on closed database
  "Exec prepared statement fails after database close (cache enabled)"
  (let ((s (call-with-database
            ":memory:"
            (lambda (db)
              (exec (sql db "create table cache(k,v);"))
              (prepare db "insert into cache values('jml', 'oak');")))))
    (exec s)))
 (test       ;;  operation on closed database
  "Exec prepared statement fails after database close (cache disabled)"
  #t
  (parameterize ((prepared-cache-size 0))
    (let* ((db0 #f)
           (s (call-with-database
               ":memory:"
               (lambda (db)
                 (set! db0 db)
                 (exec (sql db "create table cache(k,v);"))
                 (prepare db "insert into cache values('jml', 'oak');")))))
      (and (database-closed? db0)
           ; fixme: should check that s is a statement here           
           (handle-exceptions exn #t
             (and (= 1 (exec s))
                  #f))))))

 (test
  ;;  Should error with operation on closed database
  "Operating on transient statement fails after database close"
  #t
  (let* ((db0 #f)
         (s (call-with-database
             ":memory:"
             (lambda (db)
               (set! db0 db)
               (exec (sql db "create table cache(k,v);"))
               (prepare-transient
                db "insert into cache values('jml', 'oak');")))))
    (and (database-closed? db0)
         ; fixme: should check that s is a statement here
         (handle-exceptions exn #t
           (and (= 1 (exec s))
                #f)))))
 (test
  "Resurrected transient statement is still transient"
  #t
  (call-with-database
   ":memory:"
   (lambda (db)
     (let ((s (prepare-transient db "select 1;")))
       (finalize s)
       (assert (finalized? s) "statement not finalized")
       (resurrect s)
       (assert (not (finalized? s)) "statement finalized")
       (finalize s)
       (begin0
           (finalized? s)
         (finalize s))))))
 )

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
            (exec (sql db "insert or ignore into a values(?,?)") ;fixme hangs
                  "foo" "bar"))))

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
 "statement traversal"
 (call-with-database
  ":memory:"
  (lambda (db)
    (let ((s (sql db (string-literal "select 1, 2 union "
                                     "select 3, 4 union "
                                     "select 5, 6;"))))
      (test "map-rows"
            '(3 7 11)
            (query (map-rows (lambda (r) (apply + r))) s))
      (test "map-rows*"
            '(3 7 11)
            (query (map-rows* +) s))
      (test "for-each-row"
            21
            (let ((sum 0))
              (query (for-each-row (lambda (r)
                                     (set! sum (+ sum (apply + r)))))
                     s)
              sum))
      (test "for-each-row*"
            21
            (let ((sum 0))
              (query (for-each-row* (lambda (x y)
                                     (set! sum (+ sum (+ x y)))))
                     s)
              sum))
      (test "fold-rows"
            44
            (query (fold-rows (lambda (r seed)
                                (+ (apply * r)
                                   seed))
                              0)
                   s))
      (test "fold-rows*"
            44
            (query (fold-rows* (lambda (x y seed)
                                 (+ (* x y)
                                    seed))
                               0)
                   s))
      ))))

(test-group
 "large integers"
 ;; note int64 range on 32-bit is -2^53 ~ 2^53-1 where 2^53=9007199254740992
 ;; note max int64 range on 64-bit is -2^62 ~ 2^62-1;
 ;;     inexact will decrease range to 2^53
 ;; note numbers egg requires exact->inexact for non-fixnum; therefore
 ;;     injudicious application on 64-bit system reduces range to 2^53
 (call-with-database ":memory:"
   (lambda (db)
     (let ((rowid 1234567890125))
       (exec (sql db "create table cache(k,v);"))
       ;; Note the hardcoded insert to ensure the value is correct.
       (exec (sql db "insert into cache(rowid,k,v) values(1234567890125, 'jimmy', 'dunno');"))
       (test (conc "last-insert-rowid on int64 rowid (fails w/ numbers) " rowid)
             rowid
             (last-insert-rowid db))
       (test (conc "retrieve row containing int64 rowid (fails w/ numbers) " rowid)
             `(,rowid "jimmy" "dunno")
             (exec (sql db "select rowid, * from cache where rowid = ?;")
                   rowid))
       (test (conc "last-insert-rowid on int64 rowid (numbers ok) " rowid)
             (cond-expand (64bit rowid) (else (exact->inexact rowid)))
             (last-insert-rowid db))
       (test (conc "retrieve row containing int64 rowid (numbers ok) " rowid)
             `(,(cond-expand (64bit rowid) (else (exact->inexact rowid))) "jimmy" "dunno")
             (exec (sql db "select rowid, * from cache where rowid = ?;")
                   rowid))))))

(test-group
 "binding types"     ;; Perhaps "large integers" test could be folded in here.
 (call-with-database
  'memory
  (lambda (db)
    (exec (sql db "create table a(k,v);"))
    (let* ((sset (sql db "insert into a(k,v) values(?,?);"))
           (sget (sql db "select v from a where k=?;"))
           (set (lambda (k v) (exec sset k v)))
           (get (lambda (k) (first-column (exec sget k)))))
      (test "insert short blob value" 1            
            (set "foo" (string->blob "barbaz")))
      (test "select short blob value" "barbaz"
            (blob->string (get "foo")))
      (test-error "invalid bound parameter type (procedure) throws error"
                  (get identity))
      (test "select null blob value"
            0
            (blob-size (query fetch-value (sql db "select x''"))))
      ))))

(test-group
 "named parameters"
 (call-with-database
  'memory
  (lambda (db)
    (define (tval params results . args)
      (test (sprintf "~a ~a -> ~s" params args results)
            (list results)
            (begin
              ;; Note: exec sql/transient to avoid using cached statements with bound parameters
              ;; (since some of our tests explicitly do not set all params).
              (apply exec (sql/transient db (string-append "insert into a(k,v) values(" params ");")) args)
              (begin0 (query fetch-rows (sql db "select k,v from a where k=?") (car results))
                (exec (sql db "delete from a where k=?") (car results))))))
    (define (terr params . args)
      (test-error (sprintf "~a ~a -> error" params args)
                  (apply exec (sql/transient db (string-append "insert into a(k,v) values(" params ");")) args)))

    (exec (sql db "create table a(k,v);"))
    (tval "?,?"       '(1 2)   1 2)
    (tval "?1,?2"     '(3 4)   3 4)
    (tval "?2,?1"     '(5 6)   6 5)
    (tval ":foo,:bar" '(7 8)   foo: 7 bar: 8)
    (tval ":bar,:foo" '(9 10)  foo: 10 bar: 9)
    (tval "?3,?1"     '(11 12) 12 13 11)
    (terr ":foo,:bar"          foo: 30)         ;; arity error
    (terr ":foo,:bar"          fuu: 30 bar: 31) ;; name error
    
    ;; Mixed use; works, but perhaps should be avoided (and behavior unspecified?)
    (test-group
     "mixed named and positional"
     (tval ":foo,?"    '(13 14) foo: 13 14)
     (tval ":foo,?"    '(16 ()) 15 foo: 16)
     (tval "?,:foo"    '(17 18) 17 foo: 18)

     ;; Next test changed behavior recently, so I am skipping it.
     ;; (tval ":foo,?1" '(99 99)   foo: 99)       ;; 3.7.10: success
     ;; (terr ":foo,?1"            foo: 99)       ;; 3.7.3:  error: no such param

     (tval ":foo,?1" '(100 100) 100)
     (terr "?1,:foo"            101)           ;; arity error
     (terr "?1,:foo"            foo: 102)      ;; arity error
     (tval ":foo,:bar" '(103 104) foo: 103 104)
     (tval ":foo,:bar" '(103 104) 103 104)
     (tval "?3,:foo" '(3 4) 1 2 3 4)
     (tval "?3,:foo" '(3 4) foo: 1 2 3 4)      ;; basically nonsense
     )

    ;; known bugs
    (test-group "known issues"
                (tval ":foo,:bar" '(0 ()) foo: 0 foo: 0)            ;; no error
                (terr ":foo,:bar"          foo: 1 bar: 2 bar: 3)    ;; arity error.  not an important bug
                )
    
    )))

(test-group
 "schema changes"
 (call-with-database
  'memory
  (lambda (db)
    (exec (sql db "create table foo(bar,baz);"))
    (exec (sql db "insert into foo(bar,baz) values(2,3);"))
    (let ((s (sql db "select * from foo")))
      ;; Baseline tests below are not bugs, just asserts.  If we use regular 'assert'
      ;; and it fails, the test egg reports an error but doesn't include it in (test-exit).
      (test "Baseline: Initial table setup"   ;; Run statement once, obtaining column count & names.
            '((bar . 2) (baz . 3))
            (query fetch-alist s))
      (exec (sql db "alter table foo add column quux;"))
      (exec (sql db "update foo set quux=4 where bar=2;"))
      ;; If a statement is stepped and the schema has changed, SQLite re-prepares it transparently.
      ;; However, we cache the number of columns and column names in the statement handle object,
      ;; assuming they have been accessed once.  Subsequently row results will be incorrect until
      ;; the handle object is rebuilt (which will only happen if the statement is flushed from cache).
      (test "Existing statements recognize new column after ALTER TABLE ADD COLUMN"
            '((bar . 2) (baz . 3) (quux . 4))
            (query fetch-alist s))
      (exec (sql db "alter table foo rename to foo_tmp;"))
      ;; s is now expected to be invalid, as the table name changed
      (exec (sql db "create table foo as select bar as bar1, baz as baz1, quux as quux1 from foo_tmp;"))
      (test "Existing statements recognize column changes after table copy"
            '((bar1 . 2) (baz1 . 3) (quux1 . 4))
            (query fetch-alist s))
      ;; Sanity check--same behavior as previous, but with a new (sql db ...) statement instead of existing one;
      ;; the handle is still pulled from cache.
      (test "Existing statements recognize column changes after table copy (2)"
            '((bar1 . 2) (baz1 . 3) (quux1 . 4))
            (query fetch-alist (sql db "select * from foo")))
      (test "Baseline: Column changes take effect if cache bypassed"
            '((bar1 . 2) (baz1 . 3) (quux1 . 4))
            (query fetch-alist (sql db "select * from foo;")))    ;; Add semicolon so cache string=? fails
      (flush-cache! db)
      (test "Baseline: Column changes take effect after cache flush"
            '((bar1 . 2) (baz1 . 3) (quux1 . 4))
            (query fetch-alist s)))))
 (call-with-database
  'memory
  (lambda (db)
    (exec (sql db "create table foo2(bar,baz);"))
    (exec (sql db "insert into foo2(bar,baz) values(2,3);"))
    (let ((s (prepare db "select * from foo2")))
      ;; Done by definition in fetch-alist, but we need to open the statement for the next test.
      (test "Baseline: Test column count of prepared statement" 2 (column-count s))
      (test "Baseline: Test column names of prepared statement"
            '(bar baz)
            (column-names s))      
      (exec (sql db "alter table foo2 add column quux;"))
      ;; Test that column count is correct even though statement has not been reset.
      ;; A possible (incorrect) implementation is to invalidate cached column data after reset
      ;; but permit caching while statement is open and inactive, even though schema may still change.
      ;; Note that these next two tests will NOT register a change, because the statement
      ;; is only re-prepared during sqlite_step.
      (test "Column count of idle statement does not reflect ADD COLUMN"
            2
            (column-count s))
      (test "Column names of idle statement do not reflect ADD COLUMN"
            '(bar baz)
            (column-names s))
      ;; Stepping statement will re-prepare so we should see a change.
      ;; Also, once statement is running, schema cannot change.
      (test "Column count of running statement reflects ADD COLUMN"
            3
            (begin
              (step s)
              (column-count s)))
      (test "Column names of running statement reflects ADD COLUMN"
            '(bar baz quux)
            (begin
              (column-names s)))
      (test "Column count of reset statement reflects ADD COLUMN"
            3
            (begin
              (reset s)
              (column-count s)))
      (test "Column count of reset statement reflects ADD COLUMN"
            '(bar baz quux)
            (begin
              (column-names s)))
      )))
 )

(test-group
 "user defined functions"
 (test-group
  "scalar functions"
  (call-with-database
   'memory
   (lambda (db)
     (define (rsf! name nargs proc)
       (register-scalar-function! db name nargs proc))
     ;; type tests.  this also tests that overrides work (and don't crash immediately, at least)
     (test "pass int, receive int"
           '(4)
           (begin (rsf! "foo" 1 (lambda (x) (+ x 1)))
                  (exec (sql db "select foo(3);"))))
     (test "pass double, receive double"
           '(3.6)
           (begin (rsf! "foo" 1 (lambda (x) (+ x 0.5)))
                  (exec (sql db "select foo(3.1);"))))
     (test "pass null, receive null"
           '(())
           (begin (rsf! "foo" 1 (lambda (x)
                                  (unless (null? x) (error 'foo "expected null"))
                                  '()))
                  (exec (sql db "select foo(NULL);"))))
     (test "pass string, receive string"
           '("bar baz quux")
           (begin (rsf! "foo" 1 (lambda (x) (string-append x " quux")))
                  (exec (sql db "select foo('bar baz');"))))

     (test "pass null blob, receive null blob"
           (list (make-blob 0))
           (begin (rsf! "foo" 1 (lambda (x)
                                  (unless (= 0 (blob-size x)) (error 'foo "expected null blob"))
                                  (make-blob 0)))
                  (exec (sql db "select foo(x'')"))))

     (test "pass blob, receive blob"
           (list (string->blob "abc\x00def\x00ghi"))
           (begin (rsf! "foo" 1 (lambda (x)
                                  (string->blob (string-append (blob->string x) "ghi"))))
                  (exec (sql db "select foo(x'6162630064656600')"))))

     (test "pass 2 args"
           '(5)
           (begin (rsf! "foo" 2 (lambda (x y) (+ x y)))
                  (exec (sql db "select foo(2,3);"))))
     (test "pass multiple args"
           '(10)
           (begin (rsf! "foo" -1 +)
                  (exec (sql db "select foo(1,2,3,4);"))))

     (test "overload argument count"
           '(11 -1 120)
           (begin (rsf! "foo" 1 (lambda (x)   (+ x 10)))
                  (rsf! "foo" 2 (lambda (x y) (- x y)))
                  (rsf! "foo" -1 *)
                  (exec (sql db "select foo(1), foo(2,3), foo(4,5,6);"))))

     (test "unregister overloaded function"        ;; Also tests unregister in general
           '((11)
             "wrong number of arguments to function foo()"  ;; ensure this is arity error (i.e. unregister worked)
             (-1))
           (begin (rsf! "foo" 1 (lambda (x) (+ x 10)))
                  (rsf! "foo" 2 (lambda (x y) (- x y)))
                  (let ((a (exec (sql db "select foo(1);"))))
                   (rsf! "foo" 1 #f)
                   (list a
                         (handle-exceptions exn (sqlite-exception-message exn)
                           (exec (sql db "select foo(1);")))
                         (exec (sql db "select foo(2,3);"))))))

     (test "sum multiple columns"
           '((6) (15) (24))
           (begin (rsf! "summa" -1 +)
                  (query fetch-rows (sql db "select summa(a,b,c) from (select 1 as a, 2 as b, 3 as c union select 4 as a, 5 as b,6 as c union select 7 as a, 8 as b, 9 as c);"))))


     (test-error "raise error in function"
                 (begin (rsf! "foo" 1 (lambda (x) (error 'foo x)))
                        (exec (sql db "select foo(0);"))))

     
     )))

(test-group
  "aggregate functions"
  (call-with-database
   'memory
   (lambda (db)
     (define (raf! name nargs seed pstep #!optional (pfinal (lambda (x) x)))
       (register-aggregate-function! db name nargs pstep seed pfinal))
     ;; type tests.  this also tests that overrides work (and don't crash immediately, at least)
     (test "sum one column"
           '(6)
           (begin (raf! "summa" 1 0 (lambda (s x) (+ s x)))
                  (exec (sql db "select summa(a) from (select 1 as a union select 2 as a union select 3 as a);"))))
     (test "sum multiple columns"
           '((45))
           (begin (raf! "summa" -1 0 +)
                  (query fetch-rows (sql db "select summa(a,b,c) from (select 1 as a, 2 as b, 3 as c union select 4 as a, 5 as b,6 as c union select 7 as a, 8 as b, 9 as c);"))))

     ;; I'd like to test that multiple instances of an aggregate function can run simultaneously,
     ;; but am not sure how.

     (test-error "raise error in step function"
                 (begin (raf! "foo" 1 0
                              (lambda (s x) (error 'foo x))
                              (lambda (s) s))
                        (exec (sql db "select foo(5);"))))

     (test-error "raise error in final function"
                 (begin (raf! "foo" 1 0
                              (lambda (s x) (+ s x))
                              (lambda (s) (error 'foo s)))
                        (exec (sql db "select foo(5);"))))

     (test "active statements prevent unregister"
           "unable to delete/modify user-function due to active statements"
           ;; Note that we deliberately use a transient statement because
           ;; cached statements are currently flushed (finalized) when registering
           ;; functions, as a workaround for SQLite bug.  (In this case stepping the
           ;; cached statement again would fail.)
           (let ((s (prepare-transient db "select 1 union select 2 union select 3;")))
             (step s)
             (raf! "bar" 1 0 +)
             (begin0
                 (handle-exceptions exn (begin0 (sqlite-exception-message exn)
                                          (reset s)) ;; fixme: maybe really unregister bar afterward
                   (raf! "bar" 1 0 #f)
                   ;; assertion: catch erroneous case when we finalized an active statement out from
                   ;; under us, in case we are using cached statements.  can not happen currently
                   (step s))
               (finalize s))))

     (test "overload and unregister"
           '((6)      ;; 1arg
             (90)     ;; 2arg
             (11)     ;; 1arg modified
             "wrong number of arguments to function quux()"     ;; 1arg deleted
             (90))    ;; 2arg again
           (begin (raf! "quux" 1 0   (lambda (s x) (+ s x)))      ;; register fxn with 1 arg
                  (raf! "quux" 2 100 (lambda (s x y) (- s x y)))  ;; register fxn with 2 args
                  (let ((s (sql db "select quux(a) from (select 1 as a union select 2 as a union select 3 as a)"))
                        (s2 (sql db "select quux(a,b) from (select 1 as a,2 as b union select 3 as a,4 as b);")))
                    (let ((a (exec s))
                          (b (exec s2)))
                      (raf! "quux" 1 2 (lambda (s x) (+ s x 1))) ;; override existing 1 arg, changing seed and fxn
                      (let ((c (exec s)))
                        (raf! "quux" 1 0 #f) ;; delete existing 1 arg
                        (list a
                              b
                              c
                              (handle-exceptions exn (sqlite-exception-message exn)
                                (exec s))
                              (exec s2)))))))

     (test "varargs overload and unregister"
           '(((88))                     ;1arg
             ((75))                     ;3arg + 30
             ((-362880))                ;3arg * -1
             "wrong number of arguments to function mumble()"  ;3arg deleted
             ((88))                     ;1arg again
             "no such function: mumble"                        ;1arg deleted
             )
           (let ((s (sql db "select mumble(a) from (select 1 as a, 2 as b, 3 as c union select 4 as a, 5 as b,6 as c union select 7 as a, 8 as b, 9 as c);"))
                 (s3 (sql db "select mumble(a,b,c) from (select 1 as a, 2 as b, 3 as c union select 4 as a, 5 as b,6 as c union select 7 as a, 8 as b, 9 as c);")))
             (raf! "mumble" 1 100 -)
             (raf! "mumble" -1 30 +)
             (let* ((a (query fetch-rows s))
                    (b (query fetch-rows s3)))
               (raf! "mumble" -1 -1 *)
               (let ((c (query fetch-rows s3)))
                 (raf! "mumble" -1 0 #f)
                 (let ((d (handle-exceptions exn (sqlite-exception-message exn)
                           (query fetch-rows s3))))
                   (let ((e (query fetch-rows s)))
                     (raf! "mumble" 1 0 #f)
                     (let ((f (handle-exceptions exn (sqlite-exception-message exn)
                           (query fetch-rows s3))))
                       (list a b c d e f))))))))



     )))

 )

(test-group
 "multiple connections"
 (let ((db-name (create-temporary-file "db")))
   (call-with-database db-name
     (lambda (db1)
       (call-with-database db-name
         (lambda (db2)
           (exec (sql db1 "create table c(k,v);"))
           (exec (sql db1 "create table q(k,v);"))
           (exec (sql db1 "insert into c(k,v) values(?,?);") "foo" "bar")
           (exec (sql db1 "insert into c(k,v) values(?,?);") "baz" "quux")
           (let ((s (prepare db1 "select * from c;"))
                 (ic (prepare db2 "insert into c(k,v) values(?,?);"))
                 (iq (prepare db2 "insert into q(k,v) values(?,?);")))
             (test "select step in db1" '("foo" "bar") (fetch s))
             (test "insert step in db2 during select in db1 returns busy"
                   'busy
                   (begin
                     (bind-parameters iq "phlegm" "snot")
                     (handle-exceptions e
                         (cond ((sqlite-exception? e)
                                (sqlite-exception-status e))
                               (else (abort e)))
                       (step iq))))

             (test "retry the busy insert, expecting busy again"
                   ;; ensure statement is reset properly; if not, we will get a bind error
                   ;; Perform a step here to show iq is reset after BUSY in step; see next test
                   'busy
                   (handle-exceptions e
                       (cond ((sqlite-exception? e)
                              (sqlite-exception-status e))
                             (else (abort e)))
                     (step iq)))

             ;; (If we don't reset iq after BUSY--currently automatically done in step--
             ;;  then this step will mysteriously "succeed".  I suspect misuse of interface.)
             (test "different insert in db2 also returns busy"
                   'busy
                   (sqlite-exception-status
                    (handle-exceptions e e (exec ic "hyper" "meta"))))
             
             (test "another step in db1"
                   '("baz" "quux")
                   (fetch s))
             (test "another step in db1" '() (fetch s))

             (test "reset and restep read in db1 ok, insert lock was reset"
                   '("foo" "bar")
                   (begin (reset s) (fetch s)))

             ;; Old tests -- step formerly did not reset on statement BUSY
;;              (test "reset and restep read in db1, returns BUSY due to pending insert"
;;                    'busy
;;                    (sqlite-exception-status
;;                     (handle-exceptions e e (reset s) (fetch s))))

;;              (test "reset and query* fetch in s, expect BUSY, plus s should be reset by query*"
;;                    'busy
;;                    (begin
;;                      (reset s)
;;                      (sqlite-exception-status
;;                       (handle-exceptions e e (query* fetch s)))))

;;              (test "reset open db2 write, reset and restep read in db1"
;;                    '("foo" "bar")
;;                    (begin (reset iq)
;;                           (reset s)
;;                           (fetch s)))

             ;; This used to fail because the cache only supported 1 running statement at a time with
             ;; the same SQL.  Now that statements aren't cached until finalization, this works.
             (define s2 #f)
             (test "preparing statement with same SQL text as executing statement works"
                   '(("foo" "bar")
                     ("foo" "bar")
                     ("baz" "quux")
                     ("baz" "quux"))
                   (begin
                     (reset s)
                     (let ((v (fetch s)))
                       (set! s2 (prepare db1 "select * from c;"))
                       (list v
                             (fetch s2)
                             (fetch s)
                             (fetch s2)))))

             ;; Reset all statements now.  If we don't, ROLLBACK fails with
             ;; "cannot rollback transaction - SQL statements in progress".
             (reset s) (reset ic) (reset iq) (reset s2)

             ;; May be wise to pull out into separate database connections to avoid
             ;; disrupting this test.
             (test "rollback of immediate trans. releases write lock" #t
                   ;; If we do not reset the ROLLBACK statement after stepping it,
                   ;; *then* execute a read, the lock is apparently re-escalated into
                   ;; RESERVED, and not released until the ROLLBACK is reset.
                   (begin
                     (exec (sql db1 "begin immediate;"))
                     (exec (sql db1 "rollback;"))
                     (query fetch (sql db1 "select * from c;")) ;; <-- read step is required
                     (exec (sql db2 "begin exclusive;"))        ;; <-- error occurs here
                     (exec (sql db2 "rollback;"))
                     #t))

           )))))
   (delete-file db-name)))

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

(let* (
       (test-directory (or (pathname-directory ##sys#current-source-filename) "."))
       (source (make-pathname test-directory "rot13.c"))
       (shared-object (make-pathname test-directory "rot13.so"))
       (ext (pathname-strip-extension shared-object))
       (include-directory (make-pathname
			   `(,test-directory "..") "sqlite")))
  (compile-file
   source output-file: shared-object load: #f
   options: (cons
	     (string-append "-I" include-directory) (compile-file-options)))
  (test-group "extensions"
    (test
     "Calling function defined by extension"
     '("uryyb jbeyq")
     (call-with-database
      ":memory:"
      (lambda (db)
	(load-extension! db ext #f)
	(query fetch-row (sql db "select rot13('hello world')")))))
    (test-error
     "Unknown extension filename raises exception"
     (call-with-database
      ":memory:"
      (lambda (db)
	(load-extension! db (string-append test-directory "bogusobject") #f))))
    (test-error
     "Unknown extension entrypoint raises exception"
     (call-with-database
      ":memory:"
      (lambda (db)
	(load-extension! db ext "bogus_function"))))))

(include "stmt-repro.scm")
(include "cache.scm")

(test-exit)
