;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;;                                                                                        ;;;
;;;     __  .______     ______   .__   __.    .______    __    _______.                    ;;;
;;;    |  | |   _  \   /  __  \  |  \ |  |    |   _  \  |  |  /  _____|        _____       ;;;
;;;    |  | |  |_)  | |  |  |  | |   \|  |    |  |_)  | |  | |  |  __      ^..^     \9     ;;;
;;;    |  | |      /  |  |  |  | |  . `  |    |   ___/  |  | |  | |_ |     (oo)_____/      ;;;
;;;    |  | |  |\  \  |  `--'  | |  |\   |    |  |      |  | |  |__| |        WW  WW       ;;;
;;;    |__| | _| `._|  \______/  |__| \__|    | _|      |__|  \______|                     ;;;
;;;                                                                                        ;;;
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;;
;;
;; Author: Ivan Jordaan
;; Date: 2025-10-16
;; email: ivan@axoinvent.com
;; Project:
;;

(define-type packet id: 9DB0534D-F314-45AA-A1B7-B9B64DB5FE3E ID function args sub source destination)

(define (new-packet function args . sub)
  (make-packet
   ""
   function
   args
   sub
   ""
   ""))

(define (tag-packet node)
  (lambda (packet)
    (if (packet? packet)
        (make-packet
         (node-ID node)
         (packet-function packet)
         (map (tag-packet node) (packet-args packet))
         (map (tag-packet node) (packet-sub packet))
         (udp-local-socket-info (node-socket node))
         (packet-destination packet))
        packet)))

(define (execute-packet functions #!key (verbose? #f))
  (lambda (packet)
    (if verbose?
        (begin
          (display "Executing: ")
          (pp packet)))
    (if (packet? packet)
        (let ((function (table-ref functions (packet-function packet) #f)))
          (if function
              (let ((result (apply function
                                   (map (execute-packet functions verbose?: verbose?) (packet-args packet)))))
                (let loop ((sub (packet-sub packet)))
                  (if (not (null? sub))
                      (begin
                        ((execute-packet functions verbose?: verbose?) (car sub))
                        (loop (cdr sub)))))
                result)
              (begin
                (println "Unkown Function: " (packet-function packet)))))
        packet)))
