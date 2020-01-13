;;; fixup command-line options for secondary C file

(import (chicken process-context))

(define args (command-line-arguments))

(cond ((string=? "-s" (car args))
       (when (member "-static" (cdr args))
         (print "static")))
      ((string=? "-o" (car args))
       (let ((fname (cadr args)))
         (let loop ((args (cdr args)))
           (cond ((null? args))
                 ((string=? "-o" (car args))
                  (loop (cddr args)))
                 ((string=? fname (car args))
                  (loop (cdr args)))
                 (else 
                   (write-char #\space)
                   (display (car args))
                   (loop (cdr args))))))))
