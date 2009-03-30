;;; simple sqlite3 interface

;; TODO: running statements should be tracked, so as not to call
;; reset when unnecessary.  We avoid that now at the cost of
;; correctness; for example, query* doesn't reset.  And pulling a
;; cached statement out of the cache always resets, even when it
;; is not running.  It also means we could detect when users try
;; to use cached statements that are already running; we could detect
;; this in (prepare) when a statement is pulled out of the cache,
;; or we could detect it in (query); for correct behavior, we should
;; really detect it in (prepare) so that (step) behaves correctly.
;; It is not clear whether to warn and reset in this case (perhaps
;; a user accidentally left a query open) or error out.  Note that
;; it's impossible to prevent dual use of a query completely; if
;; you prepare two copies of the same statement ahead of time, you
;; can still step both copies, which actually refer to the same statement.
;; WARNING: "reset" is different from "done" -- a statement could
;; have (running?) => #t and not be reset.  This is critically important
;; distinction in STEP.

;; This type of query need only be stepped once, after which
;; it will return DONE.
;; => "INSERT INTO attached_db.temp_table SELECT * FROM attached_db.table1;"

;; LRU cache lookup (and therefore update) does not occur on
;; statement access, only on preparation.

;;; Direct-to-C

#>  #include <sqlite3.h> <#
#>
int busy_notification_handler(void *ctx, int times) {
   *(C_word*)(C_data_pointer(ctx)) = C_SCHEME_TRUE;
   return 0;
}                                                     
<#

;;; Module definition

(module sql-de-lite
;;  *
    (
     error-code error-message
     open-database close-database
     prepare prepare-transient
     finalize step ; step-through
     fetch fetch-alist
     fetch-all
     column-count column-name column-type column-data
     column-names                         ; convenience
     bind bind-parameters bind-parameter-count
     library-version                      ; string, not proc
     row-data row-alist
     reset                                ; core binding!
     call-with-database
     change-count total-change-count last-insert-rowid
     with-transaction with-deferred-transaction
     with-immediate-transaction with-exclusive-transaction
     autocommit?
     rollback commit

     set-busy-handler! busy-timeout

     ;; advanced interface
     query query* exec exec* sql

     ;; parameters
     raise-database-errors
     prepared-cache-size
   
     ;; experimental interface
     for-each-row for-each-row*
     map-rows map-rows*
     fold-rows fold-rows*

     finalized?
                
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

;;; Foreign interface

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

;;; Parameters
  
  (define raise-database-errors (make-parameter #t))
  (define prepared-cache-size (make-parameter 100))

;;; Syntax

  (define-syntax begin0                 ; multiple values discarded
    (syntax-rules () ((_ e0 e1 ...)
                      (let ((tmp e0)) e1 ... tmp))))

;;; Records

  (define-record-type sqlite-database
    (make-db ptr filename busy-handler invoked-busy-handler? statement-cache)
    db?
    (ptr db-ptr set-db-ptr!)
    (filename db-filename)
    (busy-handler db-busy-handler set-db-busy-handler!)
    (invoked-busy-handler? db-invoked-busy-handler?)
    (statement-cache db-statement-cache))
  (define-record-printer (sqlite-database db port)
    (fprintf port "#<sqlite-database ~A on ~S>"
             (or (db-ptr db)
                 "(closed)")
             (db-filename db)))
  
  (define-inline (nonnull-db-ptr db)
    (or (db-ptr db)
        (error 'sqlite3-simple "operation on closed database")))

  ;; Thin wrapper around sqlite-statement-handle, adding the two keys
  ;; which allows us to reconstitute a finalized statement.
  (define-record-type sqlite-statement
    (make-statement db sql handle)
    statement?
    (db  statement-db)
    (sql statement-sql)
    (handle statement-handle set-statement-handle!))
  (define-record-printer (sqlite-statement s p)
    (fprintf p "#<sqlite-statement ~S>"
             (statement-sql s)))

  ;; Internal record making up the guts of a prepared statement;
  ;; always embedded in a sqlite-statement.
  (define-record-type sqlite-statement-handle
    (make-handle ptr column-count column-names
                 parameter-count cached? run-state)
    handle?
    (ptr handle-ptr set-handle-ptr!)
    (column-count handle-column-count)
    (column-names handle-column-names)
    (parameter-count handle-parameter-count)
    ;; cached? flag avoids a cache-ref to check existence.
    (cached? handle-cached? set-handle-cached!)
    (run-state handle-run-state set-handle-run-state!))

  ;; Convenience accessors for guts of statement.  Should be inlined.
  (define (statement-ptr s)
    (handle-ptr (statement-handle s)))
  (define (set-statement-ptr! s p)
    (set-handle-ptr! (statement-handle s) p))
  (define (statement-column-count s)
    (handle-column-count (statement-handle s)))
  (define (statement-column-names s)
    (handle-column-names (statement-handle s)))
  (define (statement-parameter-count s)
    (handle-parameter-count (statement-handle s)))
  (define (statement-cached? s)
    (handle-cached? (statement-handle s)))
  (define (set-statement-cached! s b)
    (set-handle-cached! (statement-handle s) b))
  (define (statement-run-state s)
    (handle-run-state (statement-handle s)))
  (define (set-statement-run-state! s b)
    (set-handle-run-state! (statement-handle s) b))

  (define-inline (nonnull-statement-ptr stmt)
    ;; All references to statement ptr implicitly check for valid db.
    (or (and (nonnull-db-ptr (statement-db stmt))
             (statement-handle stmt)
             (statement-ptr stmt))
        (error 'sqlite3-simple "operation on finalized statement")))

  (define (finalized? stmt)             ; inline
    (or (not (statement-handle stmt))
        (not (statement-ptr stmt))))

;;; High-level interface  

  (define (sql db sql-str)
    (make-statement db sql-str #f))     ; (finalized? s) => #t
  
  ;; Resurrects finalized statement s or, if still live, just resets it.
  (define (resurrect! s)                ; inline
    (cond ((finalized? s)
           (let ((sn (prepare (statement-db s) (statement-sql s))))
             (set-statement-handle! s (statement-handle sn))))
          (else
           (reset s)
           (void))))

  ;; Resurrects s, binds args to s and performs a query*.  This is the
  ;; usual way to perform a query unless you need to bind arguments
  ;; manually or need other manual control.
  (define (query proc s . args)
    (resurrect! s)
    (and (apply bind-parameters s args)
         (query* proc s)))
  ;; Calls (proc s) and resets the statement immediately afterward, to
  ;; avoid locking the database.  If an exception occurs during proc,
  ;; the statement will still be reset.  Statement is NOT reset before
  ;; execution.  Note that, as closing the database will also reset any
  ;; pending statements, you can dispense with the unwind-protect as long
  ;; as you don't attempt to continue.
  (define (query* proc s)
    ;; (when (or (not (statement? s)) ; Optional check before entering
    ;;           (finalized? s))      ; exception handler.
    ;;   (error 'query* "operation on finalized statement"))
    (begin0
        (let ((c (current-exception-handler)))
          (with-exception-handler
           (lambda (ex)  ; careful not to throw another exception in here
             (when (statement? s)
               (and-let* ((h (statement-handle s))
                          (ptr (handle-ptr h)))
                 (sqlite3_reset ptr)))
             (c ex))
           (lambda () (proc s))))
        (reset s)))

  ;; Resurrects s, binds args to s and performs an exec*.
  (define (exec s . args)
    (resurrect! s)
    (and (apply bind-parameters s args)
         (exec* s)))
  ;; Executes statement s, returning the number of changes (if the
  ;; result set has no columns as in INSERT, DELETE) or the first row (if
  ;; column data is returned as in SELECT).  Resurrection is omitted, as it
  ;; would wipe out any bindings.  Reset is NOT done beforehand.
  ;; Reset afterward is not guaranteed; it is done only if a row
  ;; was returned and fetch did not throw an error.  An error in step
  ;; should not leave the statement open, but an error in retrieving column
  ;; data will (such as a string > 16MB)--this is a flaw.
  (define (exec* s)
    (and-let* ((v (fetch s)))
      (when (pair? v) (reset s))
      (if (> (column-count s) 0)
          v
          (change-count (statement-db s)))))
  
  ;; Statement traversal.  These return a lambda suitable for use
  ;; in the proc slot of query.  They call fetch repeatedly
  ;; to grab entire rows, passing them to proc.
  (define (for-each-row proc)
    (lambda (s)
      (let loop ()
        (let ((x (fetch s)))
          (cond ((null? x) #t)
                (else
                 (proc x)
                 (loop)))))))
  (define (map-rows proc)
    (lambda (s)
      (let loop ((L '()))
        (let ((x (fetch s)))
          (cond ((null? x) (reverse L))
                (else
                 (loop (cons (proc x) L))))))))
  (define (fold-rows kons knil)
    (lambda (s)
      (let loop ((xs knil))
        (let ((x (fetch s)))
          (cond ((null? x) xs)
                (else
                 (loop (kons x xs))))))))
  ;; In the starred versions, proc gets one arg for each column.
  ;; Users can use match-lambda to achieve the same effect.
  (define (for-each-row* proc)
    (for-each-row (lambda (r) (apply proc r))))
  (define (map-rows* proc)
    (map-rows (lambda (r) (apply proc r))))
  (define (fold-rows* proc)
    (fold-rows (lambda (r) (apply proc r))))

  ;; These produce equivalent results:  
  ;; (query (map-rows car) (sql db "select name, sql from sqlite_master;"))
  ;; (map car (query fetch-all (sql db "select name, sql from sqlite_master;")))

  ;; These produce equivalent results:
  ;; 
  ;; (query (for-each-row* (lambda (name sql)
  ;;                         (print "table: " name " sql: " sql ";")))
  ;;        (sql db "select name, sql from sqlite_master;"))
  ;; (query (for-each-row (match-lambda ((name sql)
  ;;                         (print "table: " name " sql: " sql ";"))))
  ;;        (sql db "select name, sql from sqlite_master;"))  

;;; Experimental
  (define (print-schema db)
    (for-each (lambda (x) (print x ";")) (schema db)))
  (define (schema db)
    (query (map-rows car)
           (sql db "select sql from sqlite_master where sql not NULL;")))

;;; Lowlevel interface

  ;; Internal.  Returns a statement-handle suitable for embedding in
  ;; a statement record.
  ;; (Note: May return #f even on SQLITE_OK, which means the statement
  ;; contained only whitespace and comments and nothing was compiled.)
  (define (prepare-handle db sql)
    (let-location ((stmt (c-pointer "sqlite3_stmt")))
      (let retry ((times 0))
        (reset-busy! db)
        (let ((rv (sqlite3_prepare_v2 (nonnull-db-ptr db)
                                      sql
                                      (string-length sql)
                                      (location stmt)
                                      #f)))
          (cond ((= rv status/ok)
                 (if stmt
                     (let* ((ncol (sqlite3_column_count stmt))
                            (nparam (sqlite3_bind_parameter_count stmt))
                            (names (make-vector ncol #f)))
                       (make-handle stmt ncol names nparam
                                    #f #f)) ; cached? run-state
                     #f))     ; not an error, even when raising errors
                ((= rv status/busy)
                 (let ((bh (db-busy-handler db)))
                   (if (and bh
                            (retry-busy? db)
                            (bh db times))
                       (retry (+ times 1))
                       (database-error db 'prepare sql))))
                (else
                 (database-error db 'prepare sql)))))))

  ;; Looks up a prepared statement in the statement cache.  If not
  ;; found, it prepares a statement and caches it.  Statements pulled
  ;; from cache are reset so that all statements prepared by this
  ;; interface are at the beginning of their program.  Statements are
  ;; also marked as cached, so FINALIZE is a no-op.
  (define (prepare db sql)
    (let ((c (db-statement-cache db)))
      (cond ((lru-cache-ref c sql)
             => reset)  ; Cache hit -should- imply non-finalized
            ((prepare-handle db sql)
             => (lambda (h)
                  (let ((s (make-statement db sql h)))
                    (when (> (lru-cache-capacity c) 0)
                      (set-handle-cached! h #t)
                      (lru-cache-set! c sql s))
                    s)))
            (else #f))))
  
  ;; Bypass cache when preparing statement.  Might occasionally be
  ;; useful, but this call may also be removed.
  (define (prepare-transient db sql)
    (make-statement db sql (prepare-handle db sql)))

  ;; Returns #f on error, 'row on SQLITE_ROW, 'done on SQLITE_DONE.
  ;; On error, statement is reset.  However, statement is not
  ;; currently reset on busy.  Oddly, one of the benefits of
  ;; resetting on error is a more descriptive error message; although
  ;; step() returns result codes directly with prepare_v2(), it still
  ;; takes a reset to convert "constraint failed" into "column key is
  ;; not unique".
  (define (step stmt)
    (let ((db (statement-db stmt)))
      (let retry ((times 0))
        (reset-busy! db)
        (let ((rv (sqlite3_step (nonnull-statement-ptr stmt))))
          (cond ((= rv status/row)
                 (set-statement-run-state! stmt 'running)
                 'row)
                ((= rv status/done) 'done)
                ((= rv status/misuse) ;; Error code/msg may not be set! :(
                 (reset stmt)
                 (error 'step "misuse of interface"))
                ;; sqlite3_step handles SCHEMA error itself.
                ((= rv status/busy)
                 (let ((bh (db-busy-handler db)))
                   (if (and bh
                            (retry-busy? db)
                            (bh db times))
                       (retry (+ times 1))
                       (database-error db 'step stmt))))
                (else
                 (reset stmt)
                 (database-error db 'step stmt)))))))

  ;; Finalize a statement.  Finalizing a finalized statement or a
  ;; cached statement is a no-op.  Finalizing a statement on a closed
  ;; DB is also a no-op; it is explicitly checked for here [*],
  ;; although normally the cache prevents this issue.  All statements
  ;; are automatically finalized when the database is closed, and cached
  ;; statements are finalized as they expire, so it is rarely necessary
  ;; to call this.
  (define (finalize stmt)
    (or (statement-cached? stmt)
        (finalize-transient stmt)))
  ;; Finalize a statement now, regardless of its cached status.  The
  ;; statement is not removed from the cache.  Finalization is indicated
  ;; by #f in the statement-handle pointer slot.
  (define (finalize-transient stmt)     ; internal
    (or (not (statement-ptr stmt))
        (not (db-ptr (statement-db stmt))) ; [*]
        (let ((rv (sqlite3_finalize
                   (nonnull-statement-ptr stmt)))) ; checks db here
          (set-statement-ptr! stmt #f)
          (cond ((= rv status/abort)
                 (database-error
                  (statement-db stmt) 'finalize))
                ((= rv status/misuse)
                 (error 'finalize "misuse of interface"))
                (else #t)))))

  ;; Resets statement STMT.  Returns: STMT.
  ;; sqlite3_reset only returns an error if the statement experienced
  ;; an error, for compatibility with sqlite3_prepare.  We get the
  ;; error from sqlite3_step, so ignore any error here.
  (define (reset stmt)
    (when (statement-run-state stmt)
      (reset-unconditionally stmt))
    stmt)
  (define (reset-unconditionally stmt)
    (sqlite3_reset (nonnull-statement-ptr stmt))
    (set-statement-run-state! stmt #f)
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
    (let ((ptr (nonnull-statement-ptr stmt)))
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
                   (else
                    (error 'bind "invalid argument type" x)))))
        (cond ((= rv status/ok) stmt)
              (else (database-error (statement-db stmt) 'bind))))))
  
  (define bind-parameter-count statement-parameter-count)

  (define (change-count db)
    (sqlite3_changes (nonnull-db-ptr db)))
  (define (total-change-count db)
    (sqlite3_total_changes (nonnull-db-ptr db)))
  (define (last-insert-rowid db)
    (sqlite3_last_insert_rowid (nonnull-db-ptr db)))
  (define column-count statement-column-count)
  (define (column-names stmt)
    (let loop ((i 0) (L '()))
      (let ((c (column-count stmt)))
        (if (>= i c)
            (reverse L)
            (loop (+ i 1) (cons (column-name stmt i) L))))))
  (define (column-name stmt i)    ;; Get result set column names, lazily.
    (let ((v (statement-column-names stmt)))
      (or (vector-ref v i)
          (let ((name (string->symbol
                       (sqlite3_column_name (nonnull-statement-ptr stmt)
                                            i))))
            (vector-set! v i name)
            name))))
  (define (column-type stmt i)
    ;; can't be cached, only valid for current row
    (int->type (sqlite3_column_type (nonnull-statement-ptr stmt) i)))
  (define (column-data stmt i)
    (let ((stmt-ptr (nonnull-statement-ptr stmt)))
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

  ;; Step statement and return row data. Returns #f (or error) on failure,
  ;; '() on done, '(col1 col2 ...) on success.
  (define (fetch s)
    (and-let* ((rv (step s)))
      (case rv
        ((done) '())
        ((row) (row-data s))
        (else
         (error 'fetch "internal error: step result invalid" rv)))))
  ;; Same as fetch, but returns an alist: '((name1 . col1) ...)
  (define (fetch-alist s)               ; nearly identical to (fetch)
    (and-let* ((rv (step s)))
      (case rv
        ((done) '())
        ((row) (row-alist s))
        (else
         (error 'fetch "internal error: step result invalid" rv)))))
  
  ;; Fetch remaining rows into a list.
  (define (fetch-all s)
    (let loop ((L '()))
      (match (fetch s)
             (() (reverse L))
             (#f (raise-database-error (statement-db s) 'fetch-all))
             (p  (loop (cons p L))))))

;;   (define (step-through stmt)
;;     (let loop ()
;;       (case (step stmt)
;;         ((row)  (loop))
;;         ((done) 'done)                  ; stmt?
;;         (else #f))))

;;; Database
  
  ;; If errors are off, user can't retrieve error message as we
  ;; return #f instead of db; though it's probably SQLITE_CANTOPEN.
  ;; Perhaps this should always throw an error.
  ;; NULL (#f) filename allowed, creates private on-disk database,
  ;; same as "".
  ;; Allows symbols 'memory => ":memory:" and 'temp or 'temporary => ""
  ;; as filename.
  (define (open-database filename)
    (let ((filename
           (if (string? filename)
               (##sys#expand-home-path filename)
               (case filename
                 ((memory) ":memory:")
                 ((temp temporary) "")
                 (else (error 'open-database "unrecognized database type"
                              filename))))))
      (let-location ((db-ptr (c-pointer "sqlite3")))
        (let* ((rv (sqlite3_open (##sys#expand-home-path filename)
                                 (location db-ptr))))
          (if (eqv? rv status/ok)
              (make-db db-ptr
                       filename
                       #f                       ; busy-handler
                       (object-evict (vector #f)) ; invoked-busy?
                       (make-lru-cache (prepared-cache-size)
                                       string=?
                                       (lambda (sql stmt)
                                         (finalize-transient stmt))))
              (if db-ptr
                  (database-error (make-db db-ptr filename #f #f #f)
                                  'open-database filename)
                  (error 'open-database "internal error: out of memory")))))))

  (define (close-database db)
    (let ((db-ptr (nonnull-db-ptr db)))
      (lru-cache-flush! (db-statement-cache db))
      (do ((stmt (sqlite3_next_stmt db-ptr #f) ; finalize pending statements
                 (sqlite3_next_stmt db-ptr stmt)))
          ((not stmt))
        (warning (sprintf "finalizing pending statement: ~S"
                          (sqlite3_sql stmt)))
        (sqlite3_finalize stmt))
      (cond ((eqv? status/ok (sqlite3_close db-ptr))
             (set-db-ptr! db #f)
             (object-release (db-invoked-busy-handler? db))
             #t)
            (else #f))))

  (define (call-with-database filename proc)
    (let ((db (open-database filename)))
      (let ((c (current-exception-handler)))
        (begin0
            (with-exception-handler
             (lambda (ex) (close-database db) (c ex))
             (lambda () (proc db)))
          (close-database db)))))
  
  (define (error-code db)
    (int->status (sqlite3_errcode (nonnull-db-ptr db))))
  (define (error-message db)
    (sqlite3_errmsg (nonnull-db-ptr db)))
  (define (database-error db where . args)
    (and (raise-database-errors)
         (apply raise-database-error db where args)))
  (define (raise-database-error db where . args)
    (apply error where (error-message db) args))

;;; Transactions

  ;; Escaping or re-entering the dynamic extent of THUNK will not
  ;; affect the in-progress transaction.  However, if an exception
  ;; occurs, or THUNK returns #f, the transaction will be rolled back.
  ;; A rollback failure is a critical error and you should likely abort.
  (define with-transaction
    (let ((tsqls '((deferred . "begin deferred;")
                   (immediate . "begin immediate;")
                   (exclusive . "begin exclusive;"))))
      (lambda (db thunk #!optional (type 'deferred))
        (and (exec (sql db (or (alist-ref type tsqls)
                               (error 'with-transaction
                                      "invalid transaction type" type))))
             (let ((rv 
                    (handle-exceptions ex (begin (or (rollback db)
                                                     (error 'with-transaction
                                                            "rollback failed"))
                                                 (signal ex))
                      (let ((rv (thunk))) ; only 1 return value allowed
                        (and rv
                             (commit db)  ; maybe warn on #f
                             rv)))))
               (or rv
                   (if (rollback db)
                       #f
                       (error 'with-transaction "rollback failed"))))))))
  
  (define with-deferred-transaction with-transaction) ; convenience fxns
  (define (with-immediate-transaction db thunk)
    (with-transaction db thunk 'immediate))
  (define (with-exclusive-transaction db thunk)
    (with-transaction db thunk 'exclusive))

  (define (autocommit? db)
    (sqlite3_get_autocommit (nonnull-db-ptr db)))

  ;; Rollback current transaction.  Reset pending statements before
  ;; doing so; rollback will fail if queries are running.  Resetting
  ;; only open queries would be nicer, but we don't track them (yet).
  ;; Rolling back when no transaction is active returns #t.
  (define (rollback db)
    (cond ((autocommit? db) #t)
          (else
           (reset-running-queries! db)
           (exec (sql db "rollback;")))))
  ;; Same behavior as rollback.
  (define (commit db)
    (cond ((autocommit? db) #t)
          (else
           (reset-running-queries! db)
           (exec (sql db "commit;")))))
  (define (reset-running-queries! db)
    (let ((db-ptr (nonnull-db-ptr db)))
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
    (vector-ref (db-invoked-busy-handler? db) 0))
  (define (reset-busy! db)
    (vector-set! (db-invoked-busy-handler? db) 0 #f))
  (define (set-busy-handler! db proc)
    (set-db-busy-handler! db proc)
    (if proc
        (sqlite3_busy_handler (nonnull-db-ptr db)
                              (foreign-value "busy_notification_handler"
                                             c-pointer)
                              (object->pointer
                               (db-invoked-busy-handler? db)))
        (sqlite3_busy_handler (nonnull-db-ptr db) #f #f))
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

  )  ; module
