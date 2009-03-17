;; (critical) BUSY return from COMMIT not handled
;; pysqlite bench at http://oss.itsystementwicklung.de/trac/pysqlite/wiki/PysqliteTwoBenchmarks

#>  #include <sqlite3.h> <#
#>
int busy_notification_handler(void *ctx, int times) {
   *(C_word*)(C_data_pointer(ctx)) = C_SCHEME_TRUE;
   return 0;
}                                                     
<#

(module sqlite3-simple
  (

   ;; FFI, for testing
   sqlite3_open sqlite3_close sqlite3_exec
                sqlite3_errcode sqlite3_extended_errcode
                sqlite3_prepare_v2

   ;; API
   error-code error-message
   open-database close-database
   prepare
   execute execute-sql
   fetch fetch-alist
   raise-database-errors
   raise-database-error
   finalize step  ; step-through
   column-count column-name column-type column-data
   bind bind-parameters bind-parameter-count
   library-version ; string, not proc
   row-data row-alist
   reset   ; core binding!
   call-with-database
   call-with-prepared-statement call-with-prepared-statements
   change-count total-change-count last-insert-rowid
   with-transaction with-deferred-transaction
   with-immediate-transaction with-exclusive-transaction
   autocommit?
   rollback commit

   set-busy-timeout! set-busy-handler! busy-timeout-handler 
   retry-busy? reset-busy! ; internal

   ;; syntax
   let-prepare

   ;; debugging
   int->status status->int             
                
                )

  (import scheme
          (except chicken reset))
  (import (only extras fprintf sprintf))
  (require-library lolevel)
  (import (only lolevel object->pointer object-release object-evict))
  (import (only data-structures alist-ref))
  (import (only srfi-18 thread-sleep! milliseconds->time))
  (import foreign foreigners easyffi)

#>? #include "sqlite3-api.h" <#
  
  (define-foreign-enum-type (sqlite3:type int)
    (type->int int->type)
    ((integer type/integer) SQLITE_INTEGER)
    ((float   type/float)   SQLITE_FLOAT)
    ((text    type/text)    SQLITE_TEXT)
    ((blob    type/blob)    SQLITE_BLOB)
    ((null    type/null)    SQLITE_NULL))

  (define-foreign-enum-type (sqlite3:status int)
    (status->int int->status)
    ((ok status/ok)               SQLITE_OK)
    ((error status/error)         SQLITE_ERROR)
    ((internal status/internal)   SQLITE_INTERNAL)
    ((permission
      status/permission)          SQLITE_PERM)
    ((abort status/abort)         SQLITE_ABORT)
    ((busy status/busy)           SQLITE_BUSY)
    ((locked status/locked)       SQLITE_LOCKED)
    ((no-memory status/no-memory) SQLITE_NOMEM)
    ((read-only status/read-only) SQLITE_READONLY)
    ((interrupt status/interrupt) SQLITE_INTERRUPT)
    ((io-error status/io-error)   SQLITE_IOERR)
    ((corrupt status/corrupt)     SQLITE_CORRUPT)
    ((not-found status/not-found) SQLITE_NOTFOUND)
    ((full status/full)           SQLITE_FULL)
    ((cant-open status/cant-open) SQLITE_CANTOPEN)
    ((protocol status/protocol)   SQLITE_PROTOCOL)
    ((empty status/empty)         SQLITE_EMPTY)
    ((schema status/schema)       SQLITE_SCHEMA)
    ((too-big status/too-big)     SQLITE_TOOBIG)
    ((constraint
      status/constraint)          SQLITE_CONSTRAINT)
    ((mismatch status/mismatch)   SQLITE_MISMATCH)
    ((misuse status/misuse)       SQLITE_MISUSE)
    ((no-lfs status/no-lfs)       SQLITE_NOLFS)
    ((authorization
      status/authorization)       SQLITE_AUTH)
    ((format status/format)       SQLITE_FORMAT)
    ((range status/range)         SQLITE_RANGE)
    ((not-a-database
      status/not-a-database)      SQLITE_NOTADB)
    ((row status/row)             SQLITE_ROW)
    ((done status/done)           SQLITE_DONE))

  (define-foreign-type sqlite3:destructor-type
    (function "void" (c-pointer "void")))
  (define-foreign-variable destructor-type/transient
    sqlite3:destructor-type "SQLITE_TRANSIENT")
  (define-foreign-variable destructor-type/static
    sqlite3:destructor-type "SQLITE_STATIC")
  (define library-version (foreign-value "sqlite3_version" c-string))
  
  (define raise-database-errors (make-parameter #f))

  (define (execute-sql db sql . params)
    (and-let* ((s (prepare db sql)))
      (apply execute s params)))

  ;; DBD::SQLite:
  ;; execute steps entire statement when column count is zero,
  ;; returning number of changes.  If columns != 0, it does a step! to
  ;; prepare for fetch (but returns 0 whether data is available or
  ;; not); fetch calls step! after execution.  note that this will run
  ;; an extra step when you don't need it

  (define (execute stmt . params)
    (and (reset stmt)
         (apply bind-some-parameters stmt 1 params)
         (if (> (column-count stmt) 0)
             stmt  ; hmmm
             (and (step-through stmt)
                  (finalize stmt)
                  (change-count (sqlite-statement-db stmt))))
         ;; if returned columns = 0, step through, finalize and return # changes
         ;; otherwise, done (nb. cannot return 0 if step has not yet occurred!!)
         ))
  
  ;; returns #f on failure, '() on done, '(col1 col2 ...) on success
  ;; note: "SQL statement" is uncompiled text;
  ;; "prepared statement" is prepared compiled statement.
  ;; sqlite3 egg uses "sql" and "stmt" for these, respectively
  (define (fetch stmt)
    (and-let* ((rv (step stmt)))
      (case rv
        ((done) '())   ; could set guard to prevent further steps
        ((row) (row-data stmt))
        (else
         (error 'fetch "internal error: step result invalid" rv)))))
  (define (fetch-alist stmt)     ; nearly identical to (fetch)
    (and-let* ((rv (step stmt)))
      (case rv
        ((done) '())
        ((row) (row-alist stmt))
        (else
         (error 'fetch "internal error: step result invalid" rv)))))
  
;;   (let ((st (prepare db "select k, v from cache where k = ?;")))
;;     (do ((i 0 (+ i 1)))
;;         ((> i 100))
;;       (execute! st i)
;;       (match (fetch st)
;;              ((k v) (print (list k v)))
;;              (() (error "no such key" k))))
;;     (finalize! st))

  (define-record sqlite-statement ptr sql db
    column-count column-names parameter-count)
  (define-record-printer (sqlite-statement s p)
    (fprintf p "#<sqlite-statement ~S>"
             (sqlite-statement-sql s)))
  (define-record sqlite-database ptr filename invoked-busy-handler?)
  (define-inline (nonnull-sqlite-database-ptr db)
    (or (sqlite-database-ptr db)
        (error 'sqlite3-simple "operation on closed database")))
  (define-inline (nonnull-sqlite-statement-ptr stmt)
    ;; All references to statement ptr implicitly check for valid db.
    (or (and (nonnull-sqlite-database-ptr (sqlite-statement-db stmt))
             (sqlite-statement-ptr stmt))
        (error 'sqlite3-simple "operation on finalized statement")))
  (define-record-printer (sqlite-database db port)
    (fprintf port "#<sqlite-database ~A on ~S>"
             (or (sqlite-database-ptr db)
                 "(closed)")
             (sqlite-database-filename db)))
  ;; May return #f even on SQLITE_OK, which means the statement contained
  ;; only whitespace and comments and nothing was compiled.
  ;; BUSY may occur here.
  (define (prepare db sql)
    (let-location ((stmt (c-pointer "sqlite3_stmt")))
      (let retry ((times 0))
        (reset-busy! db)
        (let ((rv (sqlite3_prepare_v2 (nonnull-sqlite-database-ptr db)
                                      sql
                                      (string-length sql)
                                      (location stmt)
                                      #f)))
          (cond ((= rv status/ok)
                 (if stmt
                     (let* ((ncol (sqlite3_column_count stmt))
                            (nparam (sqlite3_bind_parameter_count stmt))
                            (names (make-vector ncol #f)))
                       (make-sqlite-statement stmt sql db ncol names nparam))
                     #f))     ; not an error, even when raising errors
                ((= rv status/busy)
                 (let ((bh (busy-handler)))
                   (if (and bh
                            (retry-busy? db)
                            (bh db times))
                       (retry (+ times 1))
                       (database-error db 'prepare sql))))
                (else
                 (database-error db 'prepare sql)))))))

  ;; return #f on error, 'row on SQLITE_ROW, 'done on SQLITE_DONE
  (define (step stmt)
    (let ((rv (sqlite3_step (nonnull-sqlite-statement-ptr stmt))))
      (cond ((= rv status/row) 'row)
            ((= rv status/done) 'done)
            ((= rv status/misuse)      ;; Error code/msg may not be set! :(
             (error 'step "misuse of interface"))
            ;; sqlite3_step handles SCHEMA error itself.
            (else
             (database-error (sqlite-statement-db stmt) 'step)))))

  (define (step-through stmt)
    (let loop ()
      (case (step stmt)
        ((row)  (loop))
        ((done) 'done)  ; stmt?
        (else #f))))

  ;; Can finalize return BUSY?  If so, we may have erred in assuming
  ;; we don't have to finalize immediately.  Finalizing a finalized
  ;; statement is a no-op.  Finalizing a finalized statement on a
  ;; closed DB is also a no-op; it is explicitly checked for here [*], but
  ;; if we move to tracking pending statements at the application
  ;; level it will become automatic.
  (define (finalize stmt)
    (or (not (sqlite-statement-ptr stmt))
        (not (sqlite-database-ptr (sqlite-statement-db stmt))) ; [*]
        (let ((rv (sqlite3_finalize
                   (nonnull-sqlite-statement-ptr stmt)))) ; checks db here
          (cond ((= rv status/ok)
                 (sqlite-statement-ptr-set! stmt #f)
                 #t)
                (else (database-error
                       (sqlite-statement-db stmt) 'finalize))))))

  ;; returns: statement
  (define (reset stmt)   ; duplicates core binding
    (let ((rv (sqlite3_reset (nonnull-sqlite-statement-ptr stmt))))
      (cond ((= rv status/ok) stmt)
            (else (database-error (sqlite-statement-db stmt) 'reset)))))

  (define (bind-parameters stmt . params)
    (let ((count (bind-parameter-count stmt)))
    ;; SQLITE_RANGE returned on range error; should we check against
    ;; our own bind-parameter-count first, and if so, should it be
    ;; a database error?  This is similar to calling Scheme proc
    ;; with wrong arity, so perhaps it should error out.
      (unless (= (length params) count)
        (error 'bind-parameters "wrong number of parameters, expected" count))
      (apply bind-some-parameters stmt 1 params)))

  (define (bind-some-parameters stmt offset . params)
    (let loop ((i offset) (p params))
      (cond ((null? p) stmt)
            ((bind stmt i (car p))
             (loop (+ i 1) (cdr p)))
            (else #f))))

  (define (bind-named-parameters stmt . kvs)
    (void))

  ;; 
  (define (bind stmt i x)
    (when (or (< i 1)
              (> i (bind-parameter-count stmt)))
      ;; Should we test for this (and treat as error)?
      ;; SQLite will catch this and return a range error.
      ;; An indexing error should arguably be an immediate error...
      (error 'bind "index out of range" i))
    (let ((ptr (nonnull-sqlite-statement-ptr stmt)))
      (let ((rv 
             (cond ((string? x)
                    (sqlite3_bind_text ptr i x (string-length x)
                                       destructor-type/transient))
                   ((number? x)
                    (if (exact? x)
                        (sqlite3_bind_int ptr i x)
                        (sqlite3_bind_double ptr i x)))
                   ((blob? x)
                    (sqlite3_bind_blob ptr i x (blob-size x)
                                       destructor-type/transient))
                   ((null? x)
                    (sqlite3_bind_null ptr i))
                   ;;        ((boolean? value))
                   )))
        (cond ((= rv status/ok) stmt)
              (else (database-error (sqlite-statement-db stmt) 'bind))))))
  
  (define bind-parameter-count sqlite-statement-parameter-count)

  (define (change-count db)
    (sqlite3_changes (nonnull-sqlite-database-ptr db)))
  (define (total-change-count db)
    (sqlite3_total_changes (nonnull-sqlite-database-ptr db)))
  (define (last-insert-rowid db)
    (sqlite3_last_insert_rowid (nonnull-sqlite-database-ptr db)))
  (define column-count sqlite-statement-column-count)
  (define (column-name stmt i)
    (let ((v (sqlite-statement-column-names stmt)))
      (or (vector-ref v i)
          (let ((name (string->symbol
                       (sqlite3_column_name (nonnull-sqlite-statement-ptr stmt)
                                            i))))
            (vector-set! v i name)
            name))))
  (define (column-type stmt i)
    ;; can't be cached, only valid for current row
    (int->type (sqlite3_column_type (nonnull-sqlite-statement-ptr stmt) i)))
  (define (column-data stmt i)
    (let ((stmt-ptr (nonnull-sqlite-statement-ptr stmt)))
      (case (column-type stmt i)
        ;; INTEGER type may reach 64 bits; return at least 53 significant.
        ((integer) (sqlite3_column_int64 stmt-ptr i))
        ((float)   (sqlite3_column_double stmt-ptr i))
        ((text)    (sqlite3_column_text stmt-ptr i)) ; WARNING: NULs allowed??
        ((blob)    (let ((b (make-blob (sqlite3_column_bytes stmt-ptr i)))
                         (%copy! (foreign-lambda c-pointer "C_memcpy"
                                                 scheme-pointer c-pointer int)))
                     (%copy! b (sqlite3_column_blob stmt-ptr i) (blob-size b))
                     b))
        ((null)    '())
        (else
         (error 'column-data "illegal type"))))) ; assertion

  ;; Retrieve all columns from current row.  Does not coerce DONE
  ;; to '(); instead returns NULL for all columns.
  (define (row-data stmt)
    (let ((ncol (column-count stmt)))
      (let loop ((i 0))
        (if (fx>= i ncol)
            '()
            (cons (column-data stmt i)
                  (loop (fx+ i 1)))))))

  (define (row-alist stmt)
    (let ((ncol (column-count stmt)))
      (let loop ((i 0))
        (if (fx>= i ncol)
            '()
            (cons (cons (column-name stmt i)
                        (column-data stmt i))
                  (loop (fx+ i 1)))))))

  ;; If errors are off, user can't retrieve error message as we
  ;; return #f instead of db; though it's probably SQLITE_CANTOPEN.
  ;; Perhaps this should always throw an error.
  ;; NULL (#f) filename allowed, creates private on-disk database.  
  (define (open-database filename)
    (let-location ((db-ptr (c-pointer "sqlite3")))
      (let* ((rv (sqlite3_open filename (location db-ptr))))
        (if (eqv? rv status/ok)
            (make-sqlite-database db-ptr filename (object-evict (vector #f)))
            (if db-ptr
                (database-error (make-sqlite-database db-ptr filename #f)
                                'open-database filename)
                (error 'open-database "internal error: out of memory"))))))

  ;; database-error-code?
  ;; Issue: SQLITE_MISUSE may not set error code (happens when step
  ;; off statement).  May have to explicitly catch and signal error
  ;; everywhere.
  (define (error-code db)
    (int->status (sqlite3_errcode (nonnull-sqlite-database-ptr db))))
  (define (error-message db)
    (sqlite3_errmsg (nonnull-sqlite-database-ptr db)))

  (define (database-error db where . args)
    (and (raise-database-errors)
         (apply raise-database-error db where args)))
  (define (raise-database-error db where . args)
    (apply error where (error-message db) args))

  (define (close-database db)
    (let ((db-ptr (nonnull-sqlite-database-ptr db)))
      (do ((stmt (sqlite3_next_stmt db-ptr #f) ; finalize pending statements
                 (sqlite3_next_stmt db-ptr stmt)))
          ((not stmt))
        (warning (sprintf "finalizing pending statement: ~S"
                          (sqlite3_sql stmt)))
        (sqlite3_finalize stmt))
      (cond ((eqv? status/ok (sqlite3_close db-ptr))
             (sqlite-database-ptr-set! db #f)
             (object-release (sqlite-database-invoked-busy-handler? db))
             #t)
            (else #f))))

  (define-syntax begin0   ; multiple values discarded
    (syntax-rules () ((_ e0 e1 ...)
                      (let ((tmp e0)) e1 ... tmp))))
  (define (call-with-prepared-statement db sql proc)
    (let ((stmt (prepare db sql)))
      (begin0 (proc stmt)                     ; ignore exceptions
        (finalize stmt))))
  (define (call-with-prepared-statements db sqls proc)  ; sqls is list
    (let ((stmts (map (lambda (s) (prepare db s))
                      sqls)))
      (begin0 (apply proc stmts)
        (for-each (lambda (s) (if s (finalize s)))
                  stmts))))

  (define (call-with-database filename proc)
    (let ((db (open-database filename)))
      (handle-exceptions exn
          (begin (close-database db)
                 (signal exn))
        (begin0 (proc db)
          (close-database db)))))

  ;; Escaping or re-entering the dynamic extent of THUNK will not
  ;; affect the in-progress transaction.  However, if an exception
  ;; occurs, or THUNK returns #f, the transaction will be rolled back.
  ;; A rollback failure is a critical error and you should likely abort.
  (define with-transaction
    (let ((tsqls '((deferred . "begin deferred;")
                   (immediate . "begin immediate;")
                   (exclusive . "begin exclusive;"))))
     (lambda (db thunk #!optional (type 'deferred))
       (and (execute-sql db (or (alist-ref type tsqls)
                                (error 'with-transaction
                                       "invalid transaction type" type)))
            (let ((rv 
                   (handle-exceptions ex (begin (or (rollback db)
                                                    (error 'with-transaction
                                                           "rollback failed"))
                                                (signal ex))
                     (let ((rv (thunk))) ; only 1 return value allowed
                       (and rv
                            (execute-sql db "commit;")  ; MAY FAIL WITH BUSY
                            rv)))))
              (or rv
                  (if (rollback db)
                      #f
                      (error 'with-transaction "rollback failed"))))))))
  
  (define with-deferred-transaction with-transaction)  ; convenience fxns
  (define (with-immediate-transaction db thunk)
    (with-transaction db thunk 'immediate))
  (define (with-exclusive-transaction db thunk)
    (with-transaction db thunk 'exclusive))

  (define (autocommit? db)
    (sqlite3_get_autocommit (nonnull-sqlite-database-ptr db)))

  ;; Rollback current transaction.  Reset pending statements before
  ;; doing so; rollback will fail if queries are running.  Resetting
  ;; only open queries would be nicer, but we don't track them (yet).
  ;; Rolling back when no transaction is active returns #t.
  (define (rollback db)
    (cond ((autocommit? db) #t)
          (else
           (reset-running-queries! db)
           (execute-sql db "rollback;"))))
  ;; Same behavior as rollback.
  (define (commit db)
    (cond ((autocommit? db) #t)
          (else
           (reset-running-queries! db)
           (execute-sql db "commit;"))))
  (define (reset-running-queries! db)
    (let ((db-ptr (nonnull-sqlite-database-ptr db)))
      (do ((stmt (sqlite3_next_stmt db-ptr #f)
                 (sqlite3_next_stmt db-ptr stmt)))
          ((not stmt))
        (warning (sprintf "resetting pending statement: ~S"
                          (sqlite3_sql stmt)))
        (sqlite3_reset stmt))))
  


;;; Busy handling

  ;; Busy handling is done entirely in the application, as with SRFI-18
  ;; threads it is not legal to yield within a callback.  The backoff
  ;; algorithm of sqlite3_busy_timeout is reimplemented.

  ;; SQLite can deadlock in certain situations and to avoid this will
  ;; return SQLITE_BUSY immediately rather than invoking the busy handler.
  ;; However if there is no busy handler, we cannot tell a retryable
  ;; SQLITE_BUSY from a deadlock one.  To gain deadlock protection we
  ;; register a simple busy handler which sets a flag indicating this
  ;; BUSY is retryable.  This handler writes the flag into an evicted
  ;; object in static memory so it need not call back into Scheme nor
  ;; require safe-lambda for all calls into SQLite (a performance killer!)
  
  (define (retry-busy? db)
    (vector-ref (sqlite-database-invoked-busy-handler? db) 0))
  (define (reset-busy! db)
    (vector-set! (sqlite-database-invoked-busy-handler? db) 0 #f))
  (define busy-handler (make-parameter #f))
  (define (set-busy-handler! db proc)
    (busy-handler proc)
    (if proc
        (sqlite3_busy_handler (nonnull-sqlite-database-ptr db)
                              (foreign-value "busy_notification_handler"
                                             c-pointer)
                              (object->pointer
                               (sqlite-database-invoked-busy-handler? db)))
        (sqlite3_busy_handler (nonnull-sqlite-database-ptr db) #f #f))
    (void))
  (define busy-timeout (make-parameter 0))
  (define (set-busy-timeout! db ms)
    (busy-timeout ms)
    (if (= ms 0)
        (set-busy-handler! db #f)
        (set-busy-handler! db busy-timeout-handler)))
  (define (thread-sleep!/ms ms)
    (thread-sleep!
     (milliseconds->time (+ ms (current-milliseconds)))))
  (define busy-timeout-handler
    (let* ((delays '#(1 2 5 10 15 20 25 25  25  50  50 100))
           (totals '#(0 1 3  8 18 33 53 78 103 128 178 228))
           (ndelay (vector-length delays)))
      (lambda (db count)
        (let* ((timeout (busy-timeout))
               (delay (vector-ref delays (min count (- ndelay 1))))
               (prior (if (< count ndelay)
                          (vector-ref totals count)
                          (+ (vector-ref totals (- ndelay 1))
                             (* delay (- count (- ndelay 1)))))))
          (let ((delay (if (> (+ prior delay) timeout)
                           (- timeout prior)
                           delay)))
            (cond ((<= delay 0) #f)
                  (else
                   (thread-sleep!/ms delay)
                   #t)))))))

  (define-syntax let-prepare
    (syntax-rules ()
      ((let-prepare db ((v sql))
                    e0 e1 ...)
       (call-with-prepared-statement db sql
                                     (lambda (v) e0 e1 ...)))
      ((let-prepare db ((v0 sql0) ...)
                    e0 e1 ...)
       (call-with-prepared-statements db (list sql0 ...)
                                      (lambda (v0 ...) e0 e1 ...)))))
  
  ;; (I think (void) and '() should both be treated as NULL)
  ;; careful of return value conflict with '() meaning SQLITE_DONE though
;;   (define void?
;;     (let ((v (void)))
;;       (lambda (x) (eq? v x))))
  )

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

(call-with-database ":memory:"
   (lambda (db)
     (let-prepare db ((s1 "select 1 union select 2")
                      (s2 "select 3 union select 4"))
        (list (fetch (execute s1)) (fetch (execute s2)))))) ;=> ((1) (3))

(call-with-database ":memory:" (lambda (db) (execute-sql db "create table cache(k,v);") (execute-sql db "insert into cache values('jml', 'oak');"))) ;=> 1


(let ((s (call-with-database ":memory:" (lambda (db) (execute-sql db "create table cache(k,v);") (let-prepare db ((s "insert into cache values('jml', 'oak');")) s))))) (execute s))  ;=> Error: operation on closed database

(let ((s (call-with-database ":memory:" (lambda (db) (execute-sql db "create table cache(k,v);") (let-prepare db ((s "insert into cache values('jml', 'oak');")) s))))) (execute s))  ;=> Error: operation on closed database

(call-with-database ":memory:" (lambda (db) (execute-sql db "create table cache(k,v);") (let-prepare db ((s "insert into cache values('jml', 'oak');")) (finalize s) (execute s)))) ;=> Error: operation on finalized statement

(let ((s (call-with-database ":memory:" (lambda (db) (execute-sql db "create table cache(k,v);") (prepare db "insert into cache values('jml', 'oak');"))))) (execute s))   ;=> Error: operation on closed database

(call-with-database ":memory:"
   (lambda (db)
     (execute-sql db "create table cache(k,v);")
     (let-prepare db ((s "insert into cache values('jml', 'oak');"))
                  (execute s))))  ; => 1


(call-with-database ":memory:" (lambda (db) (rollback db))) ;=> #t (Test rollback outside transaction succeeds)
(call-with-database ":memory:" (lambda (db) (commit db))) ;=> #t

|#


;;; Notes


  ;; "step SQLITE_BUSY: If the statement is a COMMIT or occurs outside
  ;; of an explicit transaction, then you can retry the statement. If
  ;; the statement is not a COMMIT and occurs within a explicit
  ;; transaction then you should rollback the transaction before
  ;; continuing."

  ;; when two threads are writing, one attempts to grab a reserved
  ;; lock and one attempts to promote reserved->exclusive, the busy
  ;; handler is not invoked for one thread, returning busy
  ;; immediately; that thread should rollback to proceed.  The exact
  ;; wording is: "Do not invoke the busy callback when trying to
  ;; promote a lock from SHARED to RESERVED."
  ;; How do we simulate this behavior
  ;; if we are doing our own busy handling (distinguish between temporary
  ;; and permanent SQLITE_BUSY return)?  You can start all write
  ;; transactions with BEGIN IMMEDIATE to alleviate (I think) but what
  ;; about general case?

;; busy handling: To know whether it is safe to retry on BUSY (i.e.
;; non deadlock) we apparently must register a sqlite3 busy handler
;; and have it set a parameter variable, whenever it is called.
;; otherwise we cannot distinguish between BUSY retry and BUSY deadlock.
;; but this requires a foreign-safe-lambda* which will slow down STEP
;; drastically.

