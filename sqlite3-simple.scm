;; missing: (type-name native-type scheme-value)

(define-syntax %optional
  (syntax-rules ()
    ((_ sym default) default)
    ((_ sym) sym)))

;; won't work :
(define-syntax %crap
  (syntax-rules ()
    ((_ ((symbol var-name) native-name var-default-value))
     ((symbol var-name) native-name var-default-value))
    ((_ ((symbol var-name) native-name))
     ((symbol var-name) native-name 'symbol))
    ((_ (symbol native-name . var-default-value))
     ((symbol symbol) native-name . var-default-value))))

(define-syntax define-foreign-enum-type
  (syntax-rules ()
    ((_ (type-name native-type ;default-value
                   )
        (to-native from-native)
        ((symbol var-name) native-name) ...)
     (begin (define-foreign-type type-name native-type to-native from-native)
            (define-foreign-variable var-name native-type native-name) ...
            (define (from-native val)
              (cond ((= val var-name) var-name) ; var-value
                    ...
                    (else default-value)))
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
                                       'type-name)))))))))))
    ;; ignored.  Chicken 4 bug where (foo (a b) ...) matches (foo (1))
    ((_ (type-name native-type) . rest)
     (%define-foreign-enum-type (type-name native-type '()) . rest))
    ))

(require-extension matchable)
(import-for-syntax matchable)
 
;; (with-renamed r (begin car cdr) body ...)
;; -> (let ((begin_ (r 'begin)) (car_ (r 'car)) (cdr_ (r 'cdr)))
;;      body ...)
(module renamed (with-renamed)
  (define-syntax with-renamed
    (lambda (f r c)
      (##sys#check-syntax 'with-renamed f '(_ _ (_ . _) . _))
      (let ((renamer (cadr f))
            (identifiers (caddr f))
            (body (cdddr f))
            (munger (lambda (x) (string->symbol
                            (string-append (symbol->string x) "_")))))
        `(,(r 'let)
          ,(map (lambda (x)
                  `(,(munger x) (,renamer ',x)))
                identifiers)
          ,@body)))))

(import-for-syntax renamed)
(define-syntax define-foreign-enum-type-2
  (lambda (f r c)
    (match
     f
     ((_ (type-name native-type default-value)
         (to-native from-native)
         enumspecs ...)
      (let ((enums (map (lambda (spec)
                          (match spec
                                 (((s v) n d) spec)
                                 (((s v) n)   `((,s ,v) ,n ',s))
                                 (((s) n d)   `((,s ,(gensym)) ,n ,d))
                                 (((s) n)     `((,s ,(gensym)) ,n ',s))
                                 ((s n d)     `((,s ,s) ,n ,d))
                                 ((s n)       `((,s ,s) ,n ',s))
                                 (else
                                  (error 'default-foreign-enum-type
                                         "error in enum spec" spec))))
                        enumspecs)))
        (with-renamed
         r (begin define cond else if let symbol? list null?
                  car cdr case bitwise-ior error =
                  define-foreign-type define-foreign-variable)

         `(,begin_
           (,define-foreign-type_ ,type-name
             ,native-type ,to-native ,from-native)
           
           ,@(map (lambda (e)
                    (match-let ([ ((s var) name d) e ])
                      `(,define-foreign-variable_ ,var ,native-type ,name)))
                  enums)

           (,define_ (,from-native val)
             (,cond_
              ,@(map (lambda (e)
                       (match-let ([ ((s var) n value) e ])
                         `((,=_ val ,var) ,value)))
                     enums)
              (,else_ ,default-value)))

           (,define_ (,to-native syms)
             (,let_ loop ((syms (,if_ (,symbol?_ syms) (,list_ syms) syms))
                          (sum 0))
               (,if_ (,null?_ syms)
                     sum
                     (loop (,cdr_ syms)
                           (,bitwise-ior_
                            sum
                            (,let_ ((val (,car_ syms)))
                              (,case_
                               val
                               ,@(map (lambda (e)
                                        (match-let ([((symbol var) n d) e])
                                          `((,symbol) ,var)))
                                      enums)
                               (,else_ (,error_ "not a member of enum" val
                                                ',type-name)))))))))
           ))))

     ; handle missing default-value
     ((_ (type-name native-type) . rest)
      `(define-foreign-enum-type-2 (,type-name ,native-type '()) ,@rest))
     )))


 (define D (lambda (f r c)
             (print f)
             (match f ((_ (type-name native-type)
                          (to-native from-native)
                          enum ...)
                       `(define-foreign-type ,type-name ,native-type ,to-native ,from-native)
            

                       ))))

 (define D (lambda (f r c) (match f (((_ (type-name native-type)) `(list type-name native-type))))))

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
