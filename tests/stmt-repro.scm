; These tests are by Andy Bennett.

; Reproduce the finalized statement issue.
; When the currently running statement becomes the oldest statement in the
; sql-de-lite statement cache it is evicted even tho' it is still in use.
; Here we reproduce the bug.
; We run a long running SELECT statement and, within that run two different,
; non overlapping, SELECT statements on a different table. There are three
; individual statements in total.
; We run the test case in various scenarios:
;  1) With a slot in the cache for each of the short running statements and the
;     long running statement as a transient statement (i.e. it doesn't use a
;     cache slot).
;  2) With a slot in the cache for each concurrently running short statement
;     and the long running statement as a transient statement.
;  3) With a slot in the cache for every statement that runs over the course of
;     the test and all statements using the cache.
;  4) With a slot in the cache for every concurrently running statement.
;  5) With a cache that is too small to simultaneously accomodate all the
;     concurrently running statements.

(use sql-de-lite test)


; Helpers
(define (run db query)
  (exec
    (sql db
	 query)))

(define (make-counter)
  (let ((counter (make-parameter 0)))
    (lambda (inc) ; Amount to increment by or #f for current value
      (let ((n (counter)))
	(if inc
	  (begin
	    (counter (+ inc (counter)))))
	n))))


; Reproduce the bug
; Returns the number of short running statements that were executed.
(define (running-stmt-evicted sql cache-size)

  (prepared-cache-size cache-size)

  (define db (open-database 'memory))

  (map
    (cut run db <>)
    '("CREATE TEMP TABLE IF NOT EXISTS \"long\" (\"x\" INTEGER PRIMARY KEY  NOT NULL );"
      "INSERT INTO \"long\" (\"x\") VALUES (1);"
      "CREATE TEMP TABLE IF NOT EXISTS \"short\" (\"x\" INTEGER PRIMARY KEY  NOT NULL );"
      "INSERT INTO \"short\" (\"x\") VALUES (1);"))

  (define counter (make-counter))

  (query
    (for-each-row
      (lambda (row)
	(exec (sql db "SELECT * FROM short;"))
	(counter 1)
	(exec (sql db "SELECT x FROM short;"))
	(counter 1)
	))
    (sql db "SELECT * FROM long;"))
  (counter #f))


; Test scenarios
(test-group
  "sql/transient for long running statement"
  (test ; Test 1 as documented above.
    "with enough cache for all non-transient statements"
    2
    (running-stmt-evicted sql/transient 2))

  (test ; Test 2 as documented above.
    "with enough cache for running non-transient statements"
    2
    (running-stmt-evicted sql/transient 1)))

(test-group
  "sql for long running statement"
  (test ; Test 3 as documented above.
    "with enough cache for all statements"
    2
    (running-stmt-evicted sql 3))

  (test ; Test 4 as documented above.
    "with enough cache for running statements"
    2
    (running-stmt-evicted sql 2))

  (test ; Test 5 as documented above.
    "with insufficient cache"
    2
    (running-stmt-evicted sql 1)))

