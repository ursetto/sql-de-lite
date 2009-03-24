;; pysqlite bench at http://oss.itsystementwicklung.de/trac/pysqlite/wiki/PysqliteTwoBenchmarks

;; A running read query will block writers; if not wrapped in a transaction,
;; this query will not be reset on error and writers will be blocked until
;; it is finalized.  let-prepare will not finalize on error; normally you
;; rely on call-with-database to close the database and finalize everything
;; pending.  However, if your program continues, statements will be open!!
;; a) perhaps STEP should reset on any sqlite error
;; b) this doesn't help if a non-sqlite error is thrown; what you would
;;    essentially need is a with-query or cursor
;; We cannot run destructors on an object immediately when it goes out
;; of scope; we have to introduce a new scope.  Therefore we can't rely on
;; cursor destruction to reset statement, so cursors don't solve the problem.

;; Does this type of query (read/write) need to be stepped multiple times
;; or reset to release a read lock?
;;  INSERT INTO attached_db.temp_table SELECT * FROM attached_db.table1;


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
   prepare prepare-transient
   execute execute-sql
   fetch fetch-alist
   fetch-all ; ?
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

   set-busy-handler! busy-timeout
   retry-busy? reset-busy! ; internal
   with-query first-row query

   ;; syntax
   let-prepare

   ;; parameters
   raise-database-errors
   prepared-cache-size
   
   ;; debugging
   int->status status->int             
                
                )

  (import scheme
          (except chicken reset))
  (import (only extras fprintf sprintf))
  (require-library lolevel srfi-18)
  (import (only lolevel object->pointer object-release object-evict))
  (import (only data-structures alist-ref))
  (import (only srfi-18 thread-sleep! milliseconds->time))
  (import foreign foreigners easyffi)
  (require-extension matchable)
  (require-extension lru-cache)

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
  
  (define raise-database-errors (make-parameter #t))
  (define prepared-cache-size (make-parameter 100))

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

  ;; cached?: whether this statement has been cached (is non-transient).
  ;;          Stored as a flag to avoid looking up the SQL in the cache.
  (define-record sqlite-statement ptr sql db
    column-count column-names parameter-count cached?)
  (define-record-printer (sqlite-statement s p)
    (fprintf p "#<sqlite-statement ~S>"
             (sqlite-statement-sql s)))
  (define-record sqlite-database
    ptr filename busy-handler invoked-busy-handler? statement-cache)
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

  ;; Looks up the prepared statement in the statement cache; if not
  ;; found, it prepares a statement and adds it to the cache.  Statements
  ;; are also marked as cached, so FINALIZE is a no-op.
  (define (prepare db sql)
    (let ((c (sqlite-database-statement-cache db)))
      (or (lru-cache-ref c sql)
          (and-let* ((s (prepare-transient db sql)))
            (when (> (lru-cache-capacity c) 0)
              (lru-cache-set! c sql s)
              (sqlite-statement-cached?-set! s #t))
            s))))
  ;; It might make sense to manually bypass the cache if you are creating
  ;; a statement which will definitely only be used once, especially a
  ;; dynamically generated one.  If you create a lot of these, transience
  ;; may prevent bumping a more important statement out of the cache
  ;; and having to re-prepare it.
  ;; --
  ;; May return #f even on SQLITE_OK, which means the statement contained
  ;; only whitespace and comments and nothing was compiled.
  ;; BUSY may occur here.
  (define (prepare-transient db sql)
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
                       (make-sqlite-statement stmt sql db
                                              ncol names nparam
                                              #f ; cached
                                              ))
                     #f))     ; not an error, even when raising errors
                ((= rv status/busy)
                 (let ((bh (sqlite-database-busy-handler db)))
                   (if (and bh
                            (retry-busy? db)
                            (bh db times))
                       (retry (+ times 1))
                       (database-error db 'prepare sql))))
                (else
                 (database-error db 'prepare sql)))))))

  ;; return #f on error, 'row on SQLITE_ROW, 'done on SQLITE_DONE
  (define (step stmt)
    (let ((db (sqlite-statement-db stmt)))
      (let retry ((times 0))
        (reset-busy! db)
        (let ((rv (sqlite3_step (nonnull-sqlite-statement-ptr stmt))))
          (cond ((= rv status/row) 'row)
                ((= rv status/done) 'done)
                ((= rv status/misuse) ;; Error code/msg may not be set! :(
                 (error 'step "misuse of interface"))
                ;; sqlite3_step handles SCHEMA error itself.
                ((= rv status/busy)
                 (let ((bh (sqlite-database-busy-handler db)))
                   (if (and bh
                            (retry-busy? db)
                            (bh db times))
                       (retry (+ times 1))
                       (database-error db 'step stmt))))
                (else
                 (database-error db 'step stmt)))))))

  (define (step-through stmt)
    (let loop ()
      (case (step stmt)
        ((row)  (loop))
        ((done) 'done)  ; stmt?
        (else #f))))

  ;; Finalize generally only returns an error if statement execution
  ;; failed; we don't need that here.  However, it can return
  ;; status/abort if the VM is interrupted, or status/misuse.  We
  ;; always disable our statement, return #t for all errors except for
  ;; abort or misuse, and throw those.

  ;; Finalizing a finalized statement or a cached statement is a
  ;; no-op.  Finalizing a statement on a closed DB is also a
  ;; no-op; it is explicitly checked for here [*], although normally
  ;; the cache prevents this issue.
  (define (finalize stmt)
    (or (sqlite-statement-cached? stmt)
        (finalize-transient stmt)))
  (define (finalize-transient stmt)      ; internal
    (or (not (sqlite-statement-ptr stmt))
        (not (sqlite-database-ptr (sqlite-statement-db stmt))) ; [*]
        (let ((rv (sqlite3_finalize
                   (nonnull-sqlite-statement-ptr stmt)))) ; checks db here
          (sqlite-statement-ptr-set! stmt #f)
          (cond ((= rv status/abort)
                 (database-error
                  (sqlite-statement-db stmt) 'finalize))
                ((= rv status/misuse)
                 (error 'finalize "misuse of interface"))
                (else #t)))))

  ;; Resets statement STMT.  Returns: STMT.
  ;; sqlite3_reset only returns an error if the statement experienced
  ;; an error, for compatibility with sqlite3_prepare.  We get the
  ;; error from sqlite3_step, so ignore any error here.
  (define (reset stmt)                  ; duplicates core binding
    (sqlite3_reset (nonnull-sqlite-statement-ptr stmt))
    stmt)

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
            (make-sqlite-database db-ptr
                                  filename
                                  #f                         ; busy-handler
                                  (object-evict (vector #f)) ; invoked-busy?
                                  (make-lru-cache (prepared-cache-size)
                                                  string=?
                                                  (lambda (sql stmt)
                                                    (finalize-transient stmt))))
            (if db-ptr
                (database-error (make-sqlite-database db-ptr filename #f #f #f)
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
      (lru-cache-flush! (sqlite-database-statement-cache db))
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
        (do ((stmts stmts (cdr stmts)))
            ((null? stmts))
          (if (car stmts)
              (finalize (car stmts)))))))

  (define (call-with-database filename proc)
    (let ((db (open-database filename)))
      (let ((c (current-exception-handler)))
        (begin0
            (with-exception-handler
             (lambda (ex) (close-database db) (c ex))
             (lambda () (proc db)))
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
  (define (set-busy-handler! db proc)
    (sqlite-database-busy-handler-set! db proc)
    (if proc
        (sqlite3_busy_handler (nonnull-sqlite-database-ptr db)
                              (foreign-value "busy_notification_handler"
                                             c-pointer)
                              (object->pointer
                               (sqlite-database-invoked-busy-handler? db)))
        (sqlite3_busy_handler (nonnull-sqlite-database-ptr db) #f #f))
    (void))
  (define (thread-sleep!/ms ms)
    (thread-sleep!
     (milliseconds->time (+ ms (current-milliseconds)))))
  ;; (busy-timeout ms) returns a procedure suitable for use in
  ;; set-busy-handler!, implementing a spinning busy timeout using the
  ;; SQLite3 busy algorithm.  Other threads may be scheduled while
  ;; this one is busy-waiting.
  (define busy-timeout
    (let* ((delays '#(1 2 5 10 15 20 25 25  25  50  50 100))
           (totals '#(0 1 3  8 18 33 53 78 103 128 178 228))
           (ndelay (vector-length delays)))
      (lambda (ms)
        (cond
         ((< ms 0) (error 'busy-timeout "timeout must be non-negative" ms))
         ((= ms 0) #f)
         (else
          (lambda (db count)
            (let* ((delay (vector-ref delays (min count (- ndelay 1))))
                   (prior (if (< count ndelay)
                              (vector-ref totals count)
                              (+ (vector-ref totals (- ndelay 1))
                                 (* delay (- count (- ndelay 1)))))))
              (let ((delay (if (> (+ prior delay) ms)
                               (- ms prior)
                               delay)))
                (cond ((<= delay 0) #f)
                      (else
                       (thread-sleep!/ms delay)
                       #t))))))))))

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

  (define (with-query stmt thunk)
    (let ((c (current-exception-handler)))
      (let ((rv 
             (with-exception-handler (lambda (ex) (reset stmt) (c ex))
                thunk)))
        (reset stmt)
        rv)))
  (define-syntax query
    (syntax-rules ()
      ((query statement e0 e1 ...)
       (with-query statement (lambda () e0 e1 ...)))))
  ;; might not be necessary with nicer syntax for with-query --
  ;; e.g. (with-query s fetch) or (query s (fetch s))
  ;; but we -could- avoid introducing an exception thunk
  (define (first-row stmt)
    (with-query stmt (lambda () (fetch stmt))))

  (define (fetch-all s)
    ;; It is possible the query is not required if database errors are off.
    ;; Reset is also not required if statement runs to completion, but
    ;; is always done by the query.
    (with-query
     s (lambda ()
         (let loop ((L '()))
           (match (fetch s)
                  (() (reverse L))
                  (#f (raise-database-error (sqlite-statement-db s)
                                            'fetch-all))
                  (p (loop (cons p L))))))))
  
  ;; (I think (void) and '() should both be treated as NULL)
  ;; careful of return value conflict with '() meaning SQLITE_DONE though
;;   (define void?
;;     (let ((v (void)))
;;       (lambda (x) (eq? v x))))
  )

;;; Notes


;; I would like to have execute take a procedure argument which
;; resets the statement after finishing, but the syntax doesn't work,
;; because execute binds parameters as well.
;; (execute s (lambda () ...)) == (with-query (execute s) (lambda () ...))
;; but (with-query (execute s 1 2 3) (lambda () ...)) ?= (execute (bind-parameters s 1 2 3) (lambda () ...)
;; I guess execute could return a -procedure- but this is gross
;; ((execute s 1 2 3) (lambda () ...))
;; ((execute s (lambda () ...)) 1 2 3)


;; Another idea: have (prepare db sql) return a procedure that, when called,
;; binds its arguments in order.
;; (let ((s (prepare db "select * from cache where key = ?;")))
;;    (s 5))
;; Looks cool because it's like a function call, but just syntactic sugar for
;; (execute s 5).  And we still have to pass a thunk in to execute in
;; a protected environment.

;; (execute ((sql db "select * from cache where k = ?;") 1)
;;   (lambda ()
;;     (fetch)))

;; (execute (sql db "
