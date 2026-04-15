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

(define (tag-packet udp-node)
  (lambda (packet)
    (if (packet? packet)
        (make-packet
         (udp-node-ID udp-node)
         (packet-function packet)
         (map (tag-packet udp-node) (packet-args packet))
         (map (tag-packet udp-node) (packet-sub packet))
         (udp-local-socket-info (udp-node-socket udp-node))
         (packet-destination packet))
        packet)))

(define (mark-packet-internal packet)
  (make-packet
   '_internal_
   (packet-function packet)
   (packet-args packet)
   (packet-sub packet)
   (packet-source packet)
   (packet-destination packet)))

(define (execute-packet functions)
  (lambda (packet)
    (if (packet? packet)
        (let ((function (table-ref functions (packet-function packet) #f)))
          (if function
              (let ((result (apply function
                                   (list-transduce
				    ($map (execute-packet functions))
				    %cons
				    (packet-args packet)))))
                (list-transduce
		 ($map (execute-packet functions))
		 %ignore
		 (packet-sub packet))
                result)              
              (println "Unkown Function: " (packet-function packet))))
        packet)))

(define connection-packet
  (new-packet '__!connection!__ (list)))
