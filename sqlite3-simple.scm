;; missing: (type-name native-type scheme-value)

(define-syntax define-foreign-enum-type
  (syntax-rules ()
    ((_ (type-name native-type)
        (to-native from-native)
        ((symbol var-name) native-name) ...)
     (begin (define-foreign-type type-name native-type to-native from-native)
            (define-foreign-variable var-name native-type native-name) ...
            (define (from-native val)
              (cond ((= val var-name) symbol) ...
                    (else '())))
            (define (to-native syms)
              (let loop ((syms (if (symbol? syms) (list syms) syms))
                         (sum 0))
                (if (null? syms)
                    sum
                    (loop (cdr syms)
                          (bitwise-ior
                           sum
                           (let ((val (car syms)))
                             (case val
                               ((symbol) var-name) ...
                               (else
                                (error "not a member of enum" val
                                       'type-name))))))))

              )

            )
     
     
     )

    ))

  (define (sqlite3:type->number syms)
    (let loop ((syms (if (symbol? syms) (list syms) syms)) (sum 0))
      (if (null? syms)
        sum
        (loop (cdr syms)
              (bitwise-ior
                sum
                (let ((val (car syms)))
                  (case val
                    ((integer) integer)
                    ((float) float)
                    ((text) text)
                    ((blob) blob)
                    ((null) null)
                    (else
                     (error "not a member of enum" val 'sqlite3:type)))))))))
  (define (number->sqlite3:type val)
    (cond ((= val integer) 'integer)
          ((= val float) 'float)
          ((= val text) 'text)
          ((= val blob) 'blob)
          ((= val null) 'null)
          (else '())))

(define-foreign-enum-type (sqlite3:type int)
  (type->int int->type)
  ((integer type/integer) "SQLITE_INTEGER")
  ((float type/float) "SQLITE_FLOAT")
  ((text type/text) "SQLITE_TEXT")
  ((blob type/blob) "SQLITE_BLOB")
  ((null type/null) "SQLITE_NULL"))

(define-foreign-enum-type (sqlite3:type int)
  (type->int int->type)
  ((integer type/integer) "SQLITE_INTEGER")
)
