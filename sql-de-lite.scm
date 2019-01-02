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
#define sqlite3_step_safe sqlite3_step
<#

;;; Module definition

(module sql-de-lite
 (
  error-code error-message
  open-database close-database
  prepare prepare-transient
  finalize resurrect
  step ; step-through
  fetch fetch-alist
  fetch-all first-column
  fetch-value
  fetch-column fetch-row fetch-rows fetch-alists
  column-count column-name column-type column-data
  column-names                         ; convenience
  bind bind-parameters bind-parameter-count bind-parameter-name
  library-version                      ; string, not proc
  row-data row-alist
  reset ;reset-unconditionally         ; core binding!
  call-with-database
  change-count total-change-count last-insert-rowid
  with-transaction with-deferred-transaction
  with-immediate-transaction with-exclusive-transaction
  autocommit?
  rollback commit

  set-busy-handler! busy-timeout

  ;; advanced interface
  query query* exec exec* sql sql/transient

  ;; parameters
  raise-database-errors
  prepared-cache-size

  ;; experimental interface
  for-each-row for-each-row*
  map-rows map-rows*
  fold-rows
  fold-rows*   ;; deprecated
  schema print-schema
  flush-cache!

  ;; exceptions
  sqlite-exception?
  sqlite-exception-status
  sqlite-exception-message

  finalized?
  database-closed?

  ;; user-defined functions
  register-scalar-function!
  register-aggregate-function!  
  )

(import scheme)

(cond-expand
  (chicken-4
   (import (except chicken reset))
   (import (only extras fprintf sprintf))
   (require-library lolevel srfi-18)
   (import (only lolevel
                 object->pointer object-release object-evict pointer=?))
   (import (only data-structures alist-ref))
   (import (only srfi-1
		 fold first second))
   (import (only srfi-18 thread-sleep!))
   (import foreign foreigners)
   (use sql-de-lite-cache))
  (else (import (chicken base) (chicken keyword) (chicken blob))
        (import (chicken condition) (chicken fixnum))
        (import (only (chicken format) fprintf sprintf))
	(import (only srfi-1
		      fold first second))
        (import srfi-18)
        (import (only (chicken memory)
                      object->pointer pointer=?))
        (import object-evict)
        (import (chicken foreign) foreigners)
        (import sql-de-lite-cache)))

;;; Foreign interface

(include "sqlite3-api.scm")

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
;; (define-syntax dprint
;;   (syntax-rules () ((_ e0 ...)
;;     (print e0 ...))))
(define-syntax dprint
  (syntax-rules () ((_ e0 ...)
                    (void))))

;;;

(define-record-type sqlite-database
  (make-db ptr filename busy-handler invoked-busy-handler? safe-step? statement-cache active-statements)
  db?
  (ptr db-ptr set-db-ptr!)
  (filename db-filename)
  (busy-handler db-busy-handler set-db-busy-handler!)
  (invoked-busy-handler? db-invoked-busy-handler? set-db-invoked-busy-handler?!)
  (safe-step? db-safe-step? set-db-safe-step!)  ;; global flag indicating step needs safe-lambda
  (statement-cache db-statement-cache)
  (active-statements db-active-statements set-db-active-statements!))
(define-record-printer (sqlite-database db port)
  (fprintf port "#<sqlite-database ~A on ~S>"
           (or (db-ptr db)
               "(closed)")
           (db-filename db)))

(define-inline (nonnull-db-ptr db)
  (or (db-ptr db)
      (error 'sql-de-lite "operation on closed database")))

;; better implemented as a set or even plain list
(cond-expand
  (chicken-4 (use srfi-69))
  (else (import srfi-69)))
(define (make-active-statements)
  (make-hash-table))
;; as a convenience, derive the database from the statement.  forward reference.
(define (add-active-statement! s)
  (hash-table-set! (car (db-active-statements (statement-db s))) s #t))
(define (remove-active-statement! s)
  ; The statement to remove may be in any transaction frame so make sure we
  ; remove it from all of them (there should only be one but we don't know
  ; where).
  (for-each
    (lambda (t)
      (hash-table-delete! t s))
    (db-active-statements (statement-db s)))
  (hash-table-delete! (car (db-active-statements (statement-db s))) s))
(define (for-each-active-statement db proc #!key (all-transactions #f))
  (for-each
    (lambda (t)
      (hash-table-walk
	t
	(lambda (k v) (proc k))))
    (if all-transactions
      (db-active-statements db)
      (list (car (db-active-statements db))))))

;; Thin wrapper around sqlite-statement-handle, adding the two keys
;; which allows us to reconstitute a finalized statement.
(define-record-type sqlite-statement
  (make-statement db sql handle transient?)
  statement?
  (db  statement-db)
  (sql statement-sql)
  (handle statement-handle set-statement-handle!)
  (transient? statement-transient? set-statement-transient!))
(define-record-printer (sqlite-statement s p)
  (fprintf p "#<sqlite-statement ~S>"
           (statement-sql s)))

;; Internal record making up the guts of a prepared statement;
;; always embedded in a sqlite-statement.
(define-record-type sqlite-statement-handle
  (make-handle ptr column-names
               parameter-count cached? run-state)
  handle?
  (ptr handle-ptr set-handle-ptr!)
  (column-names handle-column-names set-handle-column-names!)
  (parameter-count handle-parameter-count)
  ;; cached? flag indicates statement is prepared, but inactive in cache (finalization is indicated by handle/ptr #f)
  (cached? handle-cached? set-handle-cached!)
  (run-state handle-run-state set-handle-run-state!))

;; Convenience accessors for guts of statement.  Should be inlined.
(define (statement-ptr s)
  (handle-ptr (statement-handle s)))
(define (set-statement-ptr! s p)
  (set-handle-ptr! (statement-handle s) p))
(define (statement-column-names s)
  (handle-column-names (statement-handle s)))
(define (set-statement-column-names! s v)
  (set-handle-column-names! (statement-handle s) v))
(define (statement-parameter-count s)
  (handle-parameter-count (statement-handle s)))
(define (statement-run-state s)
  (handle-run-state (statement-handle s)))
(define (statement-cached? s)
  (handle-cached? (statement-handle s)))
(define (set-statement-cached! s b)
  (set-handle-cached! (statement-handle s) b))
;; use an int instead of symbol; this is internal, and avoids mutations
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
(define (statement-safe-step? s)
  (db-safe-step? (statement-db s)))      ;; just check the global safe step

(define-inline (nonnull-statement-ptr stmt)
  ;; All references to statement ptr implicitly check for valid db.
  (or (and (nonnull-db-ptr (statement-db stmt))
           (statement-handle stmt)
           (not (statement-cached? stmt))
           (statement-ptr stmt))
      (error 'sql-de-lite "operation on finalized statement")))

(define (finalized? stmt)             ; inline
  (or (not (statement-handle stmt))
      (statement-cached? stmt)
      (not (statement-ptr stmt))))      ; consider cached statement to be finalized (or change 2 uses of finalized?)

;;; High-level interface

(define (sql db sql-str)
  (make-statement db sql-str #f #f))     ; (finalized? s) => #t
(define (sql/transient db sql-str)
  (make-statement db sql-str #f #t))

;; Resurrects finalized statement s or, if still live, just resets it.
;; Returns s, which is also modified in place.
;; NOTE: resetting a live statement could be considered an error, as it could
;; cause an infinite loop in a nested query context.
(define (resurrect s)                ; inline
  (cond ((finalized? s)
         (prepare! s))            ; prepare! should probably be inlined here
        (else
         (reset s))))

;; fast version of unwind-protect*; does not use handle-exceptions
;; so it is unsafe to throw an error inside the exception handler (program will lock up).
(define-syntax fast-unwind-protect*
  (syntax-rules ()
    ((_ protected cleanup)
     (fast-unwind-protect* protected cleanup cleanup))
    ((_ protected normal abnormal)
     (begin0
         (let ((c (current-exception-handler)))
           (with-exception-handler
            (lambda (ex)
              abnormal
              (c ex))
            (lambda () protected)))
       normal))))
(define-syntax unwind-protect*
  (syntax-rules ()
    ((_ protected cleanup)
     (unwind-protect* protected cleanup cleanup))
    ((_ protected normal abnormal)
     (begin0
         (handle-exceptions exn
             (begin abnormal
                    (abort exn))
           protected)
       normal))))

;; Resurrects s, binds args to s, performs a query*, and finalizes the statement.
;; This is the usual way to perform a query unless you need to bind arguments
;; manually or need other manual control.
;; Implementation note: we don't actually call query* here.
(define (query proc s . args)
  (resurrect s)
  (fast-unwind-protect*
   (and (apply bind-parameters s args)
        (proc s))
   (finalize s)
   ;; finalize-transient might throw an error here if status/abort is returned; this
   ;; would be fatal with fast-unwind-protect*.  Perhaps raise-database-errors should be #f here.
   (when (statement? s)
     (finalize-transient s))))
;; Calls (proc s) and resets the statement immediately afterward, to
;; avoid locking the database.  If an exception occurs during proc,
;; the statement will still be reset.  Statement is NOT reset before
;; execution.  Note that, as closing the database will also reset any
;; pending statements, you can dispense with the unwind-protect as long
;; as you don't attempt to continue.
(define (query* proc s)
  ;; Warning: if you remove test for (statement? s) in fast-unwind-protect*
  ;; abnormal exit, you must test it before entry (to avoid error in error handler) like:
  ;; (unless (statement? s)
  ;;   (error 'query* "not a statement" s))
  (fast-unwind-protect*
   (proc s)
   (reset s)  ;; May be ok to check finalized? here to avoid error if user finalized in PROC.
   (when (and (statement? s) (not (finalized? s)))
     (reset-unconditionally s))))

;; Resurrects s, binds args to s and performs an exec*.
(define (exec s . args)
  (resurrect s)
  (fast-unwind-protect*
   (and (apply bind-parameters s args)
        (exec* s))
   (finalize s)
   (begin
     ;     (raise-database-errors #f)
     (finalize-transient s))))

;; Executes statement s, returning the number of changes (if the
;; result set has no columns as in INSERT, DELETE) or the first row
;; (if column data is returned as in SELECT).  Resurrection is
;; omitted, as it would wipe out any bindings.  Reset is NOT done
;; beforehand; it is cheap, but the user must reset before a bind
;; anyway.  Reset afterward is not done via unwind-protect; it will
;; be done here if a row was returned, and in step() if a database
;; error or busy occurs, but a Scheme error (such as retrieving
;; column data > 16MB) will not cause a reset.  This is a flaw,
;; but substantially faster.
(define (exec* s)
  (and-let* ((v (fetch s)))
    (reset s)
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
(define (fold-rows* kons knil)   ;; Deprecated -- inefficient.
  (fold-rows (lambda (r seed) (apply kons (append r (list seed))))
             knil))

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
      (let ((dbptr (nonnull-db-ptr db)))
        (reset-busy! db)
        (let ((rv (sqlite3_prepare_v2 dbptr
                                      sql
                                      (string-length sql)
                                      (location stmt)
                                      #f)))
          (cond ((= rv status/ok)
                 (if stmt
                     (let* ((nparam (sqlite3_bind_parameter_count stmt)))
                       (make-handle stmt
                                    #f  ; names
                                    nparam
                                    #f  ; cached?
                                    0)) ; run-state
                     ;; Not strictly an error, but to handle it properly we must
                     ;; create a dummy statement and change all statement interfaces
                     ;; to respect it; until then, we'll make it illegal.
                     (database-error db rv 'prepare sql ;; FIXME: This will show "not an error" error.
                                     "attempted to prepare whitespace or comment SQL")))
                ((= rv status/busy)
                 (let ((bh (db-busy-handler db)))
                   (if (and bh
                            (retry-busy? db)
                            (bh db times))
                       (retry (+ times 1))
                       (database-error db rv 'prepare sql))))
                (else
                 (database-error db rv 'prepare sql))))))))

;; Looks up a prepared statement in the statement cache.  If not
;; found, it prepares a statement and caches it.  If transient,
;; the cache is ignored.  An exception is
;; thrown if a statement we pulled from cache is currently running
;; (we could just warn and reset, if this causes problems).
;; PREPARE! is internal; it expects a statement S which has already
;; been allocated, and mutates the statement handle.  Returns S on
;; success or throws an error on failure (or returns #f if errors are disabled).
(define (prepare! s)
  (let* ((db  (statement-db s))
         (sql (statement-sql s))
         (c (db-statement-cache db)))
    (dprint 'prepare!)
    ;;    (lru-cache-walk c print)
    (cond ((statement-transient? s) ; don't pull transient stmts from cache, even if matching SQL available
           (let ((h (prepare-handle db sql)))
             (set-statement-handle! s h)
             (add-active-statement! s)
             s))
          ((lru-cache-remove! c sql)     ; (sql . stmt)
           => (lambda (L)
                (let ((s1 (cdr L)))
                  (dprint "pulled statement from cache " s1)
                  (set-statement-handle! s (statement-handle s1))
                  (set-statement-cached! s #f)
                  s)))
          ((prepare-handle db sql)
           => (lambda (h)
                (set-statement-handle! s h)
                (add-active-statement! s)
                s))
          (else #f)))) ; #f -> error in prepare-handle (when errors disabled).  could call database-error anyway

;; Attempt to cache statement S.
;; Caching a statement could fail when 1) statement is transient 2) cache is disabled.
(define (cache-statement! s)
  (dprint "attempting to cache statement " s)
  (and (not (statement-transient? s))
       (let* ((c (db-statement-cache (statement-db s))))
         (and (> (lru-cache-capacity c) 0)    ; Verify cache enabled here, only because reset precedes cache-add
              (begin
                (dprint "caching " s)
                (reset s)   ; must do this prior to caching; currently, reset is illegal on cached statements
                (lru-cache-add! c (statement-sql s) s)
                (set-statement-cached! s #t)
                (remove-active-statement! s)
                #t)))))

(define (prepare db sqlst)
  (resurrect (sql db sqlst)))

;; Bypass cache when preparing statement.  Might occasionally be
;; useful, but this call may also be removed.
(define (prepare-transient db sqlst)
  (resurrect (sql/transient db sqlst)))

;; Returns #f on error, 'row on SQLITE_ROW, 'done on SQLITE_DONE.
;; On error or busy, statement is reset.   Oddly, one of the benefits of
;; resetting on error is a more descriptive error message; although
;; step() returns result codes directly with prepare_v2(), it still
;; takes a reset to convert "constraint failed" into "column key is
;; not unique".
;; We do unconditionally reset on BUSY, after any
;; retries).  If we don't, we see weird behavior.  For example,
;; first obtain a read lock with a SELECT step, then step an
;; INSERT to get a BUSY; if the INSERT is not then reset, stepping
;; a different INSERT may "succeed", but not write
;; any data.  I assume that is an undetected MISUSE condition.
;; NB It should not be necessary to reset between calls to busy handler.
(define (step stmt)
  (let ((db (statement-db stmt))
        (sptr (nonnull-statement-ptr stmt))
        (step/safe (if (statement-safe-step? stmt)
                       sqlite3_step_safe
                       sqlite3_step)))
    (let retry ((times 0))
      (reset-busy! db)
      (let ((rv (step/safe sptr)))
        (cond ((= rv status/row)
               (set-statement-running! stmt)
               'row)
              ((= rv status/done)
               (set-statement-done! stmt)
               'done)
              ;; sqlite3_step handles SCHEMA error itself.
              ((= rv status/busy)
               ;; "SQLITE_BUSY can only occur before fetching the first row." --drh
               ;; Therefore, it is safe to reset on busy.
               (set-statement-running! stmt)
               (let ((bh (db-busy-handler db)))
                 (if (and bh
                          (retry-busy? db)
                          (bh db times))
                     (retry (+ times 1))
                     (begin
                       (reset-unconditionally stmt)
                       (database-error db rv 'step stmt)))))
              (else
               (reset-unconditionally stmt)
               (database-error db rv 'step stmt)))))))

;; Finalize a statement.  If the statement is transient, finalize it
;; immediately.  Otherwise, the statement is marked for caching, so
;; reset it and add it to the LRU cache.  Finalizing a finalized
;; statement is a no-op.  Finalizing a statement on a closed DB is
;; also a no-op; it is explicitly checked for here [*].  All statements are
;; automatically finalized when the database is closed, and cached
;; statements are finalized as they expire, so it is rarely necessary
;; to call this.
(define (finalize stmt)
  (or (finalized? stmt)
      (if (cache-statement! stmt)
          #t
          (finalize-transient stmt))))

;; Finalize a statement now, regardless of its cached status.  The
;; statement is not removed from the cache.  Finalization is indicated
;; by #f in the statement-handle pointer slot.
(define (finalize-transient stmt)     ; internal
  (or (not (statement-handle stmt))   ; to avoid throwing error in query/exec unwind-protect; would be a programming error
      (not (statement-ptr stmt))
      (not (db-ptr (statement-db stmt))) ; [*]
      (begin
        (dprint 'finalize-transient 'statement stmt)
        (dprint 'handle (statement-handle stmt))
        (dprint 'db (statement-db stmt))
        (dprint 'ptr (statement-ptr stmt))
        #f)
      (let ((rv (sqlite3_finalize
                 (statement-ptr stmt)))) ; don't use nonnull-statement-ptr, because that checks cached status
        (dprint 'rv rv)
        (remove-active-statement! stmt)
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
  (sqlite3_reset (nonnull-statement-ptr stmt))      ; note: resetting cached statement will fail here
  (set-statement-reset! stmt)
  ;; Invalidate the column name cache, as schema can now change, and
  ;; we have no other way to detect such.  Another option is to invalidate
  ;; when (step) changes state to running.
  (set-statement-column-names! stmt #f)
  stmt)

;; Bind all params in order to stmt, allowing keyword arguments.
;; Although we take care to give consistent results when mixing named,
;; numeric and anonymous arguments in the same statement, actually doing
;; so is not recommended.
(define (bind-parameters stmt . params)
  (let ((count (bind-parameter-count stmt)))
    (let loop ((i 1) (p params) (kw #f))
      (if kw
          (cond ((null? p)
                 (error 'bind-parameters "keyword missing value" kw))
                ((bind stmt (string-append ":" (keyword->string kw))
                       (car p))
                 (loop (+ i 1) (cdr p) #f))
                (else #f))
          (cond ((null? p)
                 (unless (= (- i 1) count)
                   ;; # of args unknown until entire params list is traversed, due to keywords.
                   (error 'bind-parameters "wrong number of parameters, expected" count))
                 stmt)
                ((keyword? (car p))
                 (loop i (cdr p) (car p)))
                ((bind stmt i (car p))
                 (loop (+ i 1) (cdr p) #f))
                (else #f))))))

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
                      (sqlite3_bind_int64 ptr i x)    ;; Required for 64-bit.  Only int needed on 32 bit.
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
(define (bind-parameter-name s i)
  ;; FIXME: possibly do domain check on index.  I believe we have to check against bind-parameter-count
  (sqlite3_bind_parameter_name (nonnull-statement-ptr s)
                               i))

(define (change-count db)
  (sqlite3_changes (nonnull-db-ptr db)))
(define (total-change-count db)
  (sqlite3_total_changes (nonnull-db-ptr db)))
(define (last-insert-rowid db)
  (sqlite3_last_insert_rowid (nonnull-db-ptr db)))
(define (column-count stmt)
  (sqlite3_column_count (nonnull-statement-ptr stmt)))
(define (column-names stmt)
  (let loop ((i 0) (L '()))
    (let ((c (column-count stmt)))
      (if (>= i c)
          (reverse L)
          (loop (+ i 1) (cons (column-name stmt i) L))))))
(define (column-name stmt i)    ;; Get result set column names, lazily.
  (let ((v (statement-column-names stmt)))
    (if v
        (or (vector-ref v i)
            (let ((name (string->symbol
                         (sqlite3_column_name (nonnull-statement-ptr stmt) i))))
              (vector-set! v i name)
              name))
        (let ((name (string->symbol
                     (sqlite3_column_name (nonnull-statement-ptr stmt) i))))
          (when (statement-running? stmt)  ;; Or, invalidate column names in (step) when switching to running.
            (let ((v (make-vector (column-count stmt) #f)))
              (vector-set! v i name)
              (set-statement-column-names! stmt v)))
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
             ;; NB: "return value of sqlite3_column_blob() for a zero-length BLOB is a NULL pointer."
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

;; Add? row-vector

;; Step statement and return row data. Returns #f (or error) on failure,
;; '() on done, '(col1 col2 ...) on success.
(define (fetch s)
  (and-let* ((rv (step s)))
    (case rv
      ((done) '())
      ((row) (row-data s))
      (else
       (error 'fetch "internal error: step result invalid" rv)))))
(define fetch-row fetch)

;; Same as fetch, but returns an alist: '((name1 . col1) ...)
(define (fetch-alist s)               ; nearly identical to (fetch)
  (and-let* ((rv (step s)))
    (case rv
      ((done) '())
      ((row) (row-alist s))
      (else
       (error 'fetch-alist "internal error: step result invalid" rv)))))

;; Fetch first column of first row, or #f if no data.
(define (fetch-value s)
  (and-let* ((rv (step s)))
    (case rv
      ((done) #f)
      ((row)
       (column-data s 0)
       ;; I believe a row with no columns can never be returned; the
       ;; above will throw an error if so.  Or we could handle it gracefully:
       ;; (and (> 0 (column-count s)) (column-data s 0))
       )
      (else
       (error 'fetch-value "internal error: step result invalid" rv)))))

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
(define fetch-rows fetch-all)

;; Lots of duplicated code here.
(define (fetch-column s)           ;; Should this be called fetch-values?  "values" may imply MV, but is more consistent.
  (let loop ((L '()))
    (let ((val (fetch-value s)))
      (cond (val
             (loop (cons val L)))
            (else (reverse L))))))
(define (fetch-alists s)
  (let loop ((L '()))
    (let ((row (fetch-alist s)))
      (cond ((null? row)
             (reverse L))
            (else
             (loop (cons row L)))))))

;; Add? vector retrieval via row-vector.

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
             filename
             (case filename
               ((memory) ":memory:")
               ((temp temporary) "")
               (else (error 'open-database "unrecognized database type"
                            filename))))))
    (let-location ((db-ptr (c-pointer "sqlite3")))
      (let* ((rv (sqlite3_open filename
                               (location db-ptr))))
        (if (eqv? rv status/ok)
            (make-db db-ptr
                     filename
                     #f                       ; busy-handler
                     (object-evict (vector #f)) ; invoked-busy?
                     #f                       ; safe-step?
                     (make-lru-cache (prepared-cache-size)
                                     string=?
                                     (lambda (sql stmt)
                                       (finalize-transient stmt)))
                     (list (make-active-statements)))
            (if db-ptr
                (database-error (make-db db-ptr filename #f #f #f #f #f) rv
                                'open-database filename)
                (error 'open-database "internal error: out of memory")))))))

(define (close-database db)
  (let ((db-ptr (nonnull-db-ptr db)))
    ;; It's not safe to finalize all open statements known to the database library,
    ;; because SQLite itself may prepare statements under the hood (e.g. with FTS) and a double
    ;; finalize is fatal.  Therefore we must track our own open statements.
    (lru-cache-flush! (db-statement-cache db))
    (for-each-active-statement db finalize-transient all-transactions: #t)
    (let ((rv (sqlite3_close db-ptr)))
      (cond ((eqv? status/ok rv)
             (set-db-ptr! db #f)
             (object-release (db-invoked-busy-handler? db))
             (set-db-invoked-busy-handler?! db 'database-closed)
             #t)
            (else
             (database-error db rv
                             'close-database db))))))

(define (database-closed? db)
  (not (db-ptr db)))

;; It may be preferable sometimes to error out when a database handle is leaked, as this
;; is generally an error now that we track all statements.
(define (call-with-database filename proc)
  (let ((db (open-database filename)))
    (begin0
        (handle-exceptions exn
            (begin
              (or (close-database db)
                  (warning "leaked open database handle" db))
              (abort exn))
          (proc db))
      (or (close-database db)
          (warning "leaked open database handle" db)))))

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
	   (begin
	     (set-db-active-statements! db (cons (make-active-statements) (db-active-statements db)))
	     #t)
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

;; Rollback all transactions.  Reset running queries before doing
;; so, as rollback would fail if read/write queries are
;; running.  Rolling back when no transaction is active returns #t.
;; As of 3.7.11, running read queries do not prevent rollback, but we
;; have no way to only reset running read/write queries.
(define (rollback db)
  (cond ((autocommit? db) #t)
	(else
	  ; Reset all the statements in all the frames and fold all the statements together in the last frame.
	  ; If length of active statements is 2 then we're good. if it's less
	  ; than 1 then we're in a non-sql-de-lite transaction. if it's greater
	  ; than 2 then we're in at least one savepoint transaction.
	  (let ((l (length (db-active-statements db))))
	    (cond
	      ((< l 2)
	       (warning "(rollback) internal error: at least one transaction frame is missing"))
	      ((> l 2)
	       (warning "(rollback) internal error: nested transactions are in progress"))))
	  (dprint "  rollback:1: currently active statements: " (db-active-statements db))
	  ; Roll up the queries into one transaction frame.
	  (set-db-active-statements!
	    db
	    (list
	      (fold
		(lambda (v s)
		  (hash-table-merge! s v))
		(car (db-active-statements db))
		(cdr (db-active-statements db)))))
	  (dprint "  rollback:2: currently active statements: " (db-active-statements db))
	  (reset-running-queries! db)
	  (dprint "  rollback:3: currently active statements: " (db-active-statements db))
	  (exec (sql db "rollback;")))))
;; Commit all transactions.  This does not roll back running queries,
;; because running read queries are acceptable, and the behavior in the
;; presence of pending write statements is unclear.  If the commit
;; fails, you can always rollback, which will reset the pending queries.
(define (commit db)
  (cond ((autocommit? db) #t)
	(else
	  ; fold all the statements together in the last frame.
	  ; If length of active statements is 2 then we're good. if it's less
	  ; than 1 then we're in a non-sql-de-lite transaction. if it's greater
	  ; than 2 then we're in at least one savepoint transaction.
	  (let ((l (length (db-active-statements db))))
	    (cond
	      ((< l 2)
	       (warning "(commit) internal error: at least one transaction frame is missing"))
	      ((> l 2)
	       (warning "(commit) internal error: nested transactions are in progress"))))
	  (dprint "  commit:1: currently active statements: " (db-active-statements db))
	  ; Roll up the queries into one transaction frame.
	  (set-db-active-statements!
	    db
	    (list
	      (fold
		(lambda (v s)
		  (hash-table-merge! s v))
		(car (db-active-statements db))
		(cdr (db-active-statements db)))))
	  (dprint "  commit:2: currently active statements: " (db-active-statements db))
	  ;; (reset-running-queries! db)
	  (exec (sql db "commit;")))))
;; Forcibly reset all running queries in the current transaction frame (tracked in the active statement list).
(define (reset-running-queries! db)
  (for-each-active-statement db
			     (lambda (s)
			       (dprint "resetting running query " s)
			       (reset-unconditionally s))))

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
  (let ((dbptr (nonnull-db-ptr db)))
    (set-db-busy-handler! db proc)
    (if proc
        (sqlite3_busy_handler dbptr
                              (foreign-value "busy_notification_handler"
                                             c-pointer)
                              (object->pointer
                               (db-invoked-busy-handler? db)))
        (sqlite3_busy_handler dbptr #f #f))
    (void)))
(define (thread-sleep!/ms ms)
  (thread-sleep! (/ ms 1000)))
;; (busy-timeout ms) returns a procedure suitable for use in
;; set-busy-handler!, implementing a spinning busy timeout using the
;; SQLite3 busy algorithm.  Other threads may be scheduled while
;; this one is busy-waiting.
;;   FIXME: socket egg has updated algorithm which respects actual
;;          elapsed time, not estimated time
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

;;; User-defined functions

(define make-gc-root           ;; Create non-finalizable GC root pointing to OBJ
  (foreign-lambda* c-pointer ((scheme-object obj))
    "void *root = CHICKEN_new_gc_root();"
    "CHICKEN_gc_root_set(root, obj);"
    "return(root);"))
(define gc-root-ref
  (foreign-lambda scheme-object CHICKEN_gc_root_ref c-pointer))
;; (define free-gc-root
;;   (foreign-lambda void CHICKEN_delete_gc_root c-pointer))

(define-inline (%callback-result ctx x)
  (cond ((string? x)
         (sqlite3_result_text ctx x (string-length x) ;; Possible FIXME: Unnecessary extra copy of x
                              destructor-type/transient))
        ((number? x)
         (if (exact? x)
             (sqlite3_result_int64 ctx x) ;; Required for 64-bit.  Only int needed on 32 bit.
             (sqlite3_result_double ctx x)))
        ((blob? x)
         (sqlite3_result_blob ctx x (blob-size x)
                              destructor-type/transient))
        ((null? x)
         (sqlite3_result_null ctx))
        ;; zeroblob is not supported
        (else
         (error 'callback "invalid result type" x))))

(define %copy! (foreign-lambda c-pointer "C_memcpy"
                               scheme-pointer c-pointer int))
(define %value-at (foreign-lambda* c-pointer (((c-pointer "sqlite3_value*") vals)
                                              (int i))
                    "return(vals[i]);"))

(define-inline %value-data
  (lambda (vals i)
    (let* ((v (%value-at vals i))
           (t (sqlite3_value_type v)))
      ;; INTEGER type may reach 64 bits; return at least 53 significant.      
      (cond ((= t type/integer) (sqlite3_value_int64 v))
            ((= t type/float)   (sqlite3_value_double v))
            ;; Just as in column-data we choose to disallow embedded NULs in text columns;
            ;; the database allows it but it may cause problems with internal functions.
            ;; This behavior could be changed if it becomes a problem.
            ((= t type/text)    (sqlite3_value_text v))
            ((= t type/null)    '())
            ((= t type/blob)
             ;; NB: "return value of sqlite3_column_blob() for a zero-length BLOB is a NULL pointer."
             (let ((b (make-blob (sqlite3_value_bytes v))))
               (%copy! b (sqlite3_value_blob v) (blob-size b))
               b))
            (else
             (error 'value-data "illegal type at index" i)))))) ; assertion

(define (parameter-data vals n)
  (let loop ((i (fx- n 1)) (L '()))
    (if (< i 0)
        L
        (loop (fx- i 1) (cons (%value-data vals i) L)))))

;;;; Scalars

;; WARNING: C_disable_interrupts is run AFTER 1 C_check_for_interrupt already occurs,
;; upon entry.

(define-record scalar-data db name proc)

(define-external (scalar_callback (c-pointer ctx) (int nvals) (c-pointer vals))
  void
  (##core#inline "C_disable_interrupts")
  (handle-exceptions exn
      (sqlite3_result_error ctx ;; (or ((condition-property-accessor 'exn 'message) exn)
                                ;;     "Unknown Scheme error")
                            ;; Recommended to include exn location and objects, but may be dangerous.  FIXME.
                            (sprintf "(~a) ~a: ~s"
                                     ((condition-property-accessor 'exn 'location) exn)
                                     ((condition-property-accessor 'exn 'message) exn)
                                     ((condition-property-accessor 'exn 'arguments) exn))
                            -1)
    (let ((data (gc-root-ref (sqlite3_user_data ctx))))
      (let ((proc (scalar-data-proc data)))
        (%callback-result ctx (apply proc (parameter-data vals nvals))))))
  (##core#inline "C_enable_interrupts"))

(define (register-scalar-function! db name nargs proc)
  (flush-cache! db)
  (set-db-safe-step! db #t)
  (cond ((not proc)
         (unregister-function! db name nargs))
        (else
         (##sys#check-string name 'register-scalar-function!)
         (##sys#check-exact nargs 'register-scalar-function!)
         (##sys#check-closure proc 'register-scalar-function!)
         (let ((dbptr (nonnull-db-ptr db))     ;; check type now before creating gc root
               (data (make-gc-root (make-scalar-data
                                    db name proc))))  ;; Note that DB and NAME are not currently used.
           (sqlite3_create_function_v2 dbptr name nargs
                                       (foreign-value "SQLITE_UTF8" int)
                                       data
                                       (foreign-value "scalar_callback" c-pointer)
                                       #f
                                       #f
                                       (foreign-value "CHICKEN_delete_gc_root" c-pointer))))))

;;;; Aggregates

(define-record aggregate-data db name pstep pfinal seed)

(define-external (aggregate_step_callback (c-pointer ctx) (int nvals) (c-pointer vals))
  void
  (##core#inline "C_disable_interrupts")
  (handle-exceptions exn
      (sqlite3_result_error ctx
                            ;; Recommended to include exn location and objects, but may be dangerous.  FIXME.
                            (sprintf "(~a) ~a: ~s"
                                     ((condition-property-accessor 'exn 'location) exn)
                                     ((condition-property-accessor 'exn 'message) exn)
                                     ((condition-property-accessor 'exn 'arguments) exn))
                            -1)
    (let ((data (gc-root-ref (sqlite3_user_data ctx))))
      (let ((seed-box 
             ((foreign-lambda* scheme-object ((c-pointer ctx) (scheme-object v))
                "void **p = (void **)sqlite3_aggregate_context(ctx, sizeof(void *));"
                "if (*p == 0) { *p = CHICKEN_new_gc_root(); CHICKEN_gc_root_set(*p, v); return(v); }"
                "else { return(CHICKEN_gc_root_ref(*p)); }")
              ;; vector may not even be used, but callbacks are extremely slow anyway.
              ;; we could alloc in C instead.  pair better?
              ctx (vector (aggregate-data-seed data)))))
        (let ((new-seed
               (apply (aggregate-data-pstep data)
                      (vector-ref seed-box 0)
                      (parameter-data vals nvals))))
          (vector-set! seed-box 0 new-seed)))))
  (##core#inline "C_enable_interrupts"))

(define-external (aggregate_final_callback (c-pointer ctx))
  void
  (##core#inline "C_disable_interrupts")
  (handle-exceptions exn
      (sqlite3_result_error ctx
                            ;; Recommended to include exn location and objects, but may be dangerous.  FIXME.
                            (sprintf "(~a) ~a: ~s"
                                     ((condition-property-accessor 'exn 'location) exn)
                                     ((condition-property-accessor 'exn 'message) exn)
                                     ((condition-property-accessor 'exn 'arguments) exn))
                            -1)
    (let ((data (gc-root-ref (sqlite3_user_data ctx))))
      (let ((seed-box
             ((foreign-lambda* scheme-object ((c-pointer ctx))
                "void **p = (void **)sqlite3_aggregate_context(ctx, sizeof(void *));"
                "C_word r;"
                "if (*p == 0) return(C_SCHEME_FALSE);"
                "r = CHICKEN_gc_root_ref(*p);"
                "CHICKEN_delete_gc_root(*p);" ;; This is probably illegal
                "return(r);")
              ctx)))
        (let ((pfinal (aggregate-data-pfinal data))
              (seed (if seed-box
                        (vector-ref seed-box 0)
                        (aggregate-data-seed data))))
          (%callback-result ctx (pfinal seed))))))
  (##core#inline "C_enable_interrupts"))

(define (register-aggregate-function! db name nargs pstep #!optional (seed 0) (pfinal (lambda (x) x)))
  ;; Flush cache unconditionally because existing statements may not be reprepared automatically
  ;; when nargs==-1, due to SQLite bug.  This ensures idle cached statements see the update.
  (flush-cache! db)   ;; Maybe we can limit this to nargs==-1 case?
  (set-db-safe-step! db #t)
  (cond ((not pstep)
         (unregister-function! db name nargs))
        (else
         (##sys#check-string name 'register-aggregate-function!)
         (##sys#check-exact nargs 'register-aggregate-function!)
         (##sys#check-closure pstep 'register-aggregate-function!)
         (##sys#check-closure pfinal 'register-aggregate-function!)
         (let* ((dbptr (nonnull-db-ptr db))   ;; check type now before creating gc root
                (data (make-gc-root (make-aggregate-data
                                     db name pstep pfinal seed))))  ;; Note that DB and NAME are not currently used.
           (let ((rv (sqlite3_create_function_v2 dbptr name nargs
                                                 (foreign-value "SQLITE_UTF8" int)
                                                 data
                                                 #f
                                                 (foreign-value "aggregate_step_callback" c-pointer)
                                                 (foreign-value "aggregate_final_callback" c-pointer)
                                                 (foreign-value "CHICKEN_delete_gc_root" c-pointer))))
             (if (= status/ok rv)
                 (void)
                 (database-error db rv 'register-aggregate-function!)))))))

(define (unregister-function! db name nargs)
  (let ((rv (sqlite3_create_function_v2 (nonnull-db-ptr db)
                                        name
                                        nargs
                                        (foreign-value "SQLITE_UTF8" int)
                                        #f #f #f #f #f)))
    (if (= status/ok rv)
        (void)
        (database-error db rv 'unregister-function!)))))  ; module

;; Copyright (c) 2009-2015 Jim Ursetto.  All rights reserved.
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
