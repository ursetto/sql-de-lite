;;; sql-de-lite interface to SQLite 3

;; Copyright (c) 2009 Jim Ursetto.  All rights reserved.
;; BSD license at end of file.

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
  *
;;   (
;;      error-code error-message
;;      open-database close-database
;;      prepare prepare-transient
;;      finalize resurrect
;;      step ; step-through
;;      fetch fetch-alist
;;      fetch-all first-column
;;      column-count column-name column-type column-data
;;      column-names                         ; convenience
;;      bind bind-parameters bind-parameter-count
;;      library-version                      ; string, not proc
;;      row-data row-alist
;;      reset ;reset-unconditionally         ; core binding!
;;      call-with-database
;;      change-count total-change-count last-insert-rowid
;;      with-transaction with-deferred-transaction
;;      with-immediate-transaction with-exclusive-transaction
;;      autocommit?
;;      rollback commit

;;      set-busy-handler! busy-timeout

;;      ;; advanced interface
;;      query query* exec exec* sql

;;      ;; parameters
;;      raise-database-errors
;;      prepared-cache-size
   
;;      ;; experimental interface
;;      for-each-row for-each-row*
;;      map-rows map-rows*
;;      fold-rows fold-rows*
;;      schema print-schema
;;      flush-cache!

;;      finalized?
                
;;      )

  (import scheme
          (except chicken reset))
  (import (only extras fprintf sprintf))
  (require-library lolevel srfi-18)
  (import (only lolevel
                object->pointer object-release object-evict pointer=?))
  (import (only data-structures alist-ref))
  (import (only srfi-18 thread-sleep! milliseconds->time))
  (import foreign foreigners easyffi)
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
  (define (statement-reset? s)
    (= 0 (handle-run-state (statement-handle s))))
  (define (statement-running? s)
    (= 1 (handle-run-state (statement-handle s))))
  (define (statement-done? s)
    (= 2 (handle-run-state (statement-handle s))))
  (define (set-statement-reset! s)
    (set-handle-run-state! (statement-handle s) 0))
  (define (set-statement-running! s)
    (set-handle-run-state! (statement-handle s) 1))
  (define (set-statement-done! s)
    (set-handle-run-state! (statement-handle s) 2))
  
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
  ;; Returns s, which is also modified in place.
  (define (resurrect s)                ; inline
    (cond ((finalized? s)
           (let ((sn (prepare (statement-db s) (statement-sql s))))
             (set-statement-handle! s (statement-handle sn))
             s))
          (else
           (reset s))))

  ;; Resurrects s, binds args to s and performs a query*.  This is the
  ;; usual way to perform a query unless you need to bind arguments
  ;; manually or need other manual control.
  (define (query proc s . args)
    (resurrect s)
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
           (lambda (ex)    ; careful not to throw another exception in here
             (and-let* ((statement? s)
                        (h (statement-handle s)) ; is this too paranoid?
                        (handle-ptr h))
               (reset-unconditionally s))
             (c ex))
           (lambda () (proc s))))
        (reset s)))

  ;; Resurrects s, binds args to s and performs an exec*.
  (define (exec s . args)
    (resurrect s)
    (and (apply bind-parameters s args)
         (exec* s)))
  ;; Executes statement s, returning the number of changes (if the
  ;; result set has no columns as in INSERT, DELETE) or the first row (if
  ;; column data is returned as in SELECT).  Resurrection is omitted, as it
  ;; would wipe out any bindings.  Reset is NOT done beforehand; it is cheap,
  ;; but the user must reset before a bind anyway.
  ;; Reset afterward is not guaranteed; it is done only if a row
  ;; was returned and fetch did not throw an error.  An error in step
  ;; should not leave the statement open, but an error in retrieving column
  ;; data will (such as a string > 16MB)--this is a flaw.

  ;; FIXME: Ultimately it looks like we will need to unwind-protect
  ;; a reset here.  If a BUSY occurs on a write, a pending lock may
  ;; be held, stopping further reads until this is reset.  It is unknown
  ;; if this also happens for read busies, nor if any other error may
  ;; cause this problem.  It is unknown if we can safely just reset on
  ;; a BUSY in step, rather than catching it here.
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
  (define (first-column row)
    (and (pair? row) (car row)))
  (define (flush-cache! db)
    (lru-cache-flush! (db-statement-cache db)))

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
                                    #f 0)) ; cached? run-state
                     #f))     ; not an error, even when raising errors
                ((= rv status/busy)
                 (let ((bh (db-busy-handler db)))
                   (if (and bh
                            (retry-busy? db)
                            (bh db times))
                       (retry (+ times 1))
                       (database-error db rv 'prepare sql))))
                (else
                 (database-error db rv 'prepare sql)))))))

  ;; Looks up a prepared statement in the statement cache.  If not
  ;; found, it prepares a statement and caches it.  An exception is
  ;; thrown if a statement we pulled from cache is currently running
  ;; (we could just warn and reset, if this causes problems).
  ;; Statements are also marked as cached, so FINALIZE is a no-op.
  (define (prepare db sql)
    (let ((c (db-statement-cache db)))
      (cond ((lru-cache-ref c sql)
             => (lambda (s)
                  (cond ((statement-running? s)
                         (error 'prepare
                                "cached statement is currently executing" s))
                        ((statement-done? s)
                         (reset s))
                        (else s))))
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
                 (set-statement-running! stmt)
                 'row)
                ((= rv status/done)
                 (set-statement-done! stmt)
                 'done)
                ;; sqlite3_step handles SCHEMA error itself.
                ((= rv status/busy)
                 (set-statement-running! stmt)
                 (let ((bh (db-busy-handler db)))
                   (if (and bh
                            (retry-busy? db)
                            (bh db times))
                       (retry (+ times 1))
                       (database-error db rv 'step stmt))))
                (else
                 (reset-unconditionally stmt)
                 (database-error db rv 'step stmt)))))))

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
                  (statement-db stmt) rv 'finalize))
                (else #t)))))

  ;; Resets statement STMT.  Returns: STMT.
  ;; sqlite3_reset only returns an error if the statement experienced
  ;; an error, for compatibility with sqlite3_prepare.  We get the
  ;; error from sqlite3_step, so ignore any error here.
  (define (reset stmt)
    (when (not (statement-reset? stmt))
      (reset-unconditionally stmt))
    stmt)
  (define (reset-unconditionally stmt)
    (sqlite3_reset (nonnull-statement-ptr stmt))
    (set-statement-reset! stmt)
    stmt)

  (define (bind-parameters stmt . params)
    (let ((count (bind-parameter-count stmt)))
      ;; SQLITE_RANGE returned on range error; should we check against
      ;; our own bind-parameter-count first, and if so, should it be
      ;; a database error?  This is similar to calling Scheme proc
      ;; with wrong arity, so perhaps it should error out.
      (unless (= (length params) count)
        (error 'bind-parameters "wrong number of parameters, expected" count))
      (let loop ((i 1) (p params))
        (cond ((null? p) stmt)
              ((bind stmt i (car p))
               (loop (+ i 1) (cdr p)))
              (else #f)))))

  ;; Bind parameter at index I of statement S to value X.  The variable
  ;; I may be an integer (the first parameter is 1, not 0) or a string
  ;; for a named parameter -- for example, "$key", ":key" or "@key".
  ;; A reference to an invalid index will throw an exception.
  (define (bind s i x)
    (if (string? i)
        (%bind-named s i x)
        (%bind-int s i x)))

  (define (%bind-named s n x)
    (##sys#check-string n 'bind-named)
    (let ((i (sqlite3_bind_parameter_index (nonnull-statement-ptr s) n)))
      (if (= i 0)
          (error 'bind-named "no such parameter name" n s)
          (%bind-int s i x))))

  (define (%bind-int stmt i x)
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
              (else (database-error (statement-db stmt) rv 'bind))))))
  
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
    (let* ((stmt-ptr (nonnull-statement-ptr stmt))
           (t (sqlite3_column_type stmt-ptr i)))  ; faster than column-type
      ;; INTEGER type may reach 64 bits; return at least 53 significant.      
      (cond ((= t type/integer) (sqlite3_column_int64 stmt-ptr i))
            ((= t type/float)   (sqlite3_column_double stmt-ptr i))
            ((= t type/text)    (sqlite3_column_text stmt-ptr i)) ; NULs OK??
            ((= t type/null)    '())
            ((= t type/blob)
             (let ((b (make-blob (sqlite3_column_bytes stmt-ptr i)))
                   (%copy! (foreign-lambda c-pointer "C_memcpy"
                                           scheme-pointer c-pointer int)))
               (%copy! b (sqlite3_column_blob stmt-ptr i) (blob-size b))
               b))
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
      (let ((row (fetch s)))
        (cond ((null? row)
               (reverse L))
              (row
               (loop (cons row L)))
              (else
               ;; Semantics are odd if exception raising is disabled.
               (error 'fetch-all "fetch failed" s))))))

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
                  (database-error (make-db db-ptr filename #f #f #f) rv
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
  (define (database-error db code where . args)
    (and (raise-database-errors)
         (apply raise-database-error db code where args)))
  (define (raise-database-error db code where . args)
    ;; status/misuse may not set the error code and message; signal
    ;; a generic misuse error if we believe that has happened.
    ;; [ref. http://www.sqlite.org/c3ref/errcode.html]
    (if (or (not (= code status/misuse))
            (eqv? (error-code db) 'misuse))
        (raise-database-error/status
         db (int->status code) where (error-message db) args)
        (raise-database-error/status
         db 'misuse where "misuse of interface" args)))
  (define (raise-database-error/status db status where message args)
    (abort
     (make-composite-condition
      (make-property-condition 'exn
                               'location where
                               'message message
                               'arguments args)
      (make-property-condition 'sqlite
                               'status status
                               'message message))))
  (define sqlite-exception? (condition-predicate 'sqlite))
  ;; note that these will return #f if you pass it a non-sqlite condition
  (define sqlite-exception-status (condition-property-accessor 'sqlite 'status))
  (define sqlite-exception-message (condition-property-accessor 'sqlite 'message))

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
                                                 (abort ex))
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

  ;; Rollback current transaction.  Reset running queries before doing
  ;; so, as rollback would fail if read or read/write queries are
  ;; running.  Rolling back when no transaction is active returns #t.
  (define (rollback db)
    (cond ((autocommit? db) #t)
          (else
           (reset-running-queries! db)
           (exec (sql db "rollback;")))))
  ;; Commit current transaction.  This does not roll back running queries,
  ;; because running read queries are acceptable, and the behavior in the
  ;; presence of pending write statements is unclear.  If the commit
  ;; fails, you can always rollback, which will reset the pending queries.
  (define (commit db)
    (cond ((autocommit? db) #t)
          (else
           ;; (reset-running-queries! db)
           (exec (sql db "commit;")))))
  ;; Reset all running queries.  A list of all prepared statements known
  ;; to the library is obtained; if a statement is found in the cache,
  ;; we call (reset) on it.  If it is not, it is a transient statement,
  ;; which we do not track; forcibly reset it as its run state is unknown.
  ;; Statements that fall off the cache have been finalized and are
  ;; consequently not known to the library.
  (define (reset-running-queries! db)
    (let ((db-ptr (nonnull-db-ptr db))
          (c (db-statement-cache db)))
      (do ((sptr (sqlite3_next_stmt db-ptr #f)
                 (sqlite3_next_stmt db-ptr sptr)))
          ((not sptr))
        (let* ((sql (sqlite3_sql sptr))
               (s (lru-cache-ref c sql)))
          (if (and s
                   (pointer=? (statement-ptr s) sptr))
              (reset-unconditionally s)   ; in case our state is out of sync
              (begin
                (fprintf
                 (current-error-port)
                 "Warning: resetting transient prepared statement: ~S\n" sql)
                (sqlite3_reset sptr)))))))

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

;; Copyright (c) 2009 Jim Ursetto.  All rights reserved.
;; 
;; Redistribution and use in source and binary forms, with or without
;; modification, are permitted provided that the following conditions are met:
;; 
;;  Redistributions of source code must retain the above copyright notice,
;;   this list of conditions and the following disclaimer.
;;  Redistributions in binary form must reproduce the above copyright notice,
;;   this list of conditions and the following disclaimer in the documentation
;;   and/or other materials provided with the distribution.
;;  Neither the name of the author nor the names of its contributors 
;;   may be used to endorse or promote products derived from this software 
;;   without specific prior written permission.
;; 
;; THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
;; AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
;; THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
;; PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDERS OR
;; CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
;; EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
;; PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
;; PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
;; LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
;; NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
;; SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
