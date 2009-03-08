;; missing: (type-name native-type scheme-value)

#>  #include <sqlite3.h> <#

(module sqlite3-simple
  (

   ;; FFI, for testing
   sqlite3_open sqlite3_close sqlite3_exec
                sqlite3_errcode sqlite3_extended_errcode
                sqlite3_prepare_v2

   ;; API
   error-code error-message
   open-database close-database
   prepare fetch
   raise-errors
   raise-database-error
   finalize step
   column-count column-type column-data
   reset   ; core binding!

   ;; debugging
   int->status status->int             
                
                )

  (import scheme chicken)
  (import (only extras fprintf sprintf))
  (import (only lolevel pointer->address move-memory!))
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

  (define raise-errors (make-parameter #f))
  
  (define (exec-sql db sql . params)
    (if (null? params)
        (int->status (sqlite3_exec (sqlite-database-ptr db) sql #f #f #f))
        (begin
          (error)
          
         
         )
        
        )
    )

  ;; name doesn't seem right.  also, may now be same as execute!.  but
  ;; execute! can't actually step the statement.  check DBD::SQLite:
  ;; execute steps entire statement when column count is zero,
  ;; returning number of changes.  If columns != 0, it does a step! to
  ;; prepare for fetch (but returns 0 whether data is available or
  ;; not); fetch calls step! after execution.  note that this will run
  ;; an extra step when you don't need it; although you probably normally
  ;; want to step through all results.

  ;; we could have fetch call step! itself prior to fetching; this means
  ;; execute! has nothing to do except for resetting the statement and
  ;; binding any parameters.
  ;; it would mean execute! is equivalent to sqlite3:bind-parameters!.
  ;; 
  (define (exec-statement stmt . params)
    (void)

    )

  ;; returns #f on failure, '() on done, '(col1 col2 ...) on success
  ;; note: "SQL statement" is uncompiled text;
  ;; "prepared statement" is prepared compiled statement.
  ;; sqlite3 egg uses "sql" and "stmt" for these, respectively
  (define (fetch stmt)
    (and-let* ((rv (step stmt)))
      (case rv
        ((done) '())
        ((row)
         

         )
        (else
         (error 'fetch "internal error: step result invalid" rv)))
      ))

;;   (let ((st (prepare db "select k, v from cache where k = ?;")))
;;     (do ((i 0 (+ i 1)))
;;         ((> i 100))
;;       (execute! st i)
;;       (match (fetch st)
;;              ((k v) (print (list k v)))
;;              (() (error "no such key" k))))
;;     (finalize! st))

  (define-record sqlite-statement ptr sql db)
  (define-record-printer (sqlite-statement s p)
    (fprintf p "#<sqlite-statement ~S>"
             (sqlite-statement-sql s)))
  (define-record sqlite-database ptr filename)
  (define-inline (nonnull-sqlite-database-ptr db)
    (or (sqlite-database-ptr db)
        (error 'sqlite3-simple "operation on closed database")))
  (define-inline (nonnull-sqlite-statement-ptr stmt)
    (or (sqlite-statement-ptr stmt)
        (error 'sqlite3-simple "operation on finalized statement")))  
  (define-record-printer (sqlite-database db port)
    (fprintf port "#<sqlite-database ~A on ~S>"
             (or (sqlite-database-ptr db)
                 "(closed)")
             (sqlite-database-filename db)))

  ;; May return #f even on SQLITE_OK, which means the statement contained
  ;; only whitespace and comments and nothing was compiled.
  (define (prepare db sql)
    (let-location ((stmt (c-pointer "sqlite3_stmt")))
      (let ((rv (sqlite3_prepare_v2 (nonnull-sqlite-database-ptr db)
                                    sql
                                    (string-length sql)
                                    (location stmt)
                                    #f)))
        (cond ((= rv status/ok)
               (if stmt
                   (make-sqlite-statement stmt sql db)
                   #f)) ; not an error, even when raising errors
              ;; ((= rv status/busy (retry))) 
              (else
               (database-error db 'prepare sql))))))

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

  (define (finalize stmt)
    (let ((rv (sqlite3_finalize (nonnull-sqlite-statement-ptr stmt))))
      (cond ((= rv status/ok) #t)
            (else (database-error (sqlite-statement-db stmt) 'finalize)))))

  (define (reset stmt)   ; duplicates core binding
    (let ((rv (sqlite3_reset (nonnull-sqlite-statement-ptr stmt))))
      (cond ((= rv status/ok) #t)
            (else (database-error (sqlite-statement-db stmt) 'reset)))))

  (define (execute stmt . params)
    (and (reset stmt)
         ;; (apply bind-parameters stmt params)
         ;; if returned columns = 0, step until done and return # changes
         ;; otherwise, done
         ))

  (define (bind-parameters stmt . params)
    (void))

  ;; 
  (define (bind stmt index param)
    (void))

  (define (column-count stmt)
    (sqlite3_column_count (nonnull-sqlite-statement-ptr stmt)))
  (define (column-type stmt i)
    (int->type
     (sqlite3_column_type (nonnull-sqlite-statement-ptr stmt) i)))
  (define (column-data stmt i)
    (let ((stmt-ptr (nonnull-sqlite-statement-ptr stmt)))
      (case (column-type stmt i)
        ((integer) (sqlite3_column_int stmt-ptr i))
        ((float)   (sqlite3_column_double stmt-ptr i))
        ((text)    (sqlite3_column_text stmt-ptr i)) ; WARNING: NULs allowed??
        ((blob)    (let ((b (make-blob (sqlite3_column_bytes stmt-ptr i))))
                     (move-memory! (sqlite3_column_blob stmt-ptr i) b)))
        ((null)    '())
        (else
         (error 'column-data "illegal type"))))) ; assertion

  ;; retrieve all columns from current row (?)
  (define (row-data stmt)
    (void)
    )

  ;; If errors are off, user can't retrieve error message as we
  ;; return #f instead of db; though it's probably SQLITE_CANTOPEN.
  ;; Perhaps this should always throw an error.
  ;; NULL (#f) filename allowed, creates private on-disk database.  
  (define (open-database filename)
    (let-location ((db-ptr (c-pointer "sqlite3")))
      (let* ((rv (sqlite3_open filename (location db-ptr)))
             (db (make-sqlite-database db-ptr filename)))
        (if (eqv? rv status/ok)
            db
            (if db-ptr
                (database-error db 'open-database filename)
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
    (and (raise-errors)
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
             #t)
            (else #f))))

  (define (call-with-prepared-statement db sql proc)
    (void)
    )
  (define (call-with-prepared-statements db sqls proc)  ; sqls is list
    (void)
    )

  ;; (I think (void) and '() should both be treated as NULL)
  ;; careful of return value conflict with '() meaning SQLITE_DONE though
;;   (define void?
;;     (let ((v (void)))
;;       (lambda (x) (eq? v x))))

  


  )

#|

(use sqlite3-simple)
(raise-errors #t)
(define db (open-database "a.db"))
(define stmt (prepare db "create table cache(key text primary key, val text);"))
(step stmt)
(step (prepare db "insert into cache values('ostrich', 'bird');"))
(step (prepare db "insert into cache values('orangutan', 'monkey');"))
(define stmt2 (prepare db "select rowid, key, val from cache;"))
(step stmt2)
(column-count stmt2)  ; => 3
(column-type stmt2 0) ; => integer
(column-type stmt2 1) ; => text
(column-type stmt2 2) ; => text
(column-data stmt2 0) ; => 1
(column-data stmt2 1) ; => "ostrich"
(column-data stmt2 2) ; => "orangutan"
|#

;;; Notes


  ;; "step SQLITE_BUSY: If the statement is a COMMIT or occurs outside
  ;; of an explicit transaction, then you can retry the statement. If
  ;; the statement is not a COMMIT and occurs within a explicit
  ;; transaction then you should rollback the transaction before
  ;; continuing."

