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
;; Date: 2025-10-21
;; email: ivan@axoinvent.com
;; Project:
;;

(define BATCH_SEND_SIZE 5000)

(define-type udp-node id: 26A52C1A-CFC5-4526-A10D-2ED793974E41 ID socket connections channels)

(define (new-udp-node address port
                      #!key
                      (ID (generate-guid))
                      (in-transducer $identity)
                      (out-transducer $identity))
  (let ((udp-node (make-udp-node
                   ID
                   (open-udp (list local-address: (string-append address ":" port)))
                   (make-table)
                   (make-table))))
    (udp-node-channels-set! udp-node
			    (make-channels (create-channel transducer: ($compose
									($filter packet?)
									($map (connection-manager udp-node))
									($filter packet?)
									in-transducer))
					   (create-channel transducer: ($compose
									out-transducer
									($filter packet?)
									($map (tag-packet udp-node))))))
    udp-node))

(define (udp-node-inbound udp-node)
  (channels-inbound (udp-node-channels udp-node)))

(define (udp-node-outbound udp-node)
  (channels-outbound (udp-node-channels udp-node)))

(define (connection-manager udp-node)
  (lambda (packet)
    (let ((ID (packet-ID packet))
	  (connections (udp-node-connections udp-node)))
      (if (table-ref connections ID #f) packet
	  (begin
	    (table-set! connections ID (packet-source packet))
	    (if (equal? (packet-function packet) '__!connection!__) (pp (string-append "New Connection"))
		packet))))))

(define (start-udp-node udp-node #!optional (input-reducer %ignore))
  (start-reader udp-node)
  (consume-channel
   (udp-node-outbound udp-node)
   (create-consumer (%processor (batch-send udp-node))))
  (consume-channel
   (udp-node-inbound udp-node)
   (create-consumer input-reducer))
  udp-node)

(define (batch-send udp-node)
  (let ((batch-size BATCH_SEND_SIZE)
        (socket (udp-node-socket udp-node)))
    (lambda (packet)
      (let ((packets (pack-packet packet batch-size))
            (target-socket-info (packet-destination packet)))
        (udp-destination-set!
         (socket-info-address target-socket-info)
         (socket-info-port-number target-socket-info)
         socket)
        (for-each (lambda (datum)
                    (write datum socket))
                  packets)))))

(define (pack-packet packet batch-size)
  (let* ((u8 (object->u8vector packet))
         (u8l (u8vector-length u8))
         (total (ceiling (/ u8l batch-size)))
         (ID (string-append (number->string u8l) (number->string (random-integer 10000000)))))
    (let loop ((index 0) (end batch-size))
      (if (= index total) (list)
          (begin
            (cons (object->u8vector
                   (list->table
                    `((ID . ,ID)
                      (index . ,index)
                      (total . ,total)
                      (payload . ,(subu8vector u8 (* index batch-size) (min u8l end))))))
                  (loop (+ 1 index) (+ batch-size end))))))))

(define (start-reader udp-node)
  (let ((cache (make-table init: #f))
        (socket (udp-node-socket udp-node)))
    (thread
     (lambda ()
       (let loop ((u8 (read socket)))
         (let* ((data (u8vector->object u8))
                (ID (table-ref data 'ID))
                (total (table-ref data 'total))
                (index (table-ref data 'index))
                (payload (table-ref data 'payload))
                (vec (or (table-ref cache ID)
                         (let ((result (make-vector (+ 1 total) #f)))
                           (table-set! cache ID result)
                           (vector-set! result 0 0)
                           result))))
           (vector-set! vec (+ 1 index) payload)
           (vector-set! vec 0 (+ 1 (vector-ref vec 0)))

           (if (= total (vector-ref vec 0))
               (begin
                 (>> (udp-node-inbound udp-node) (u8vector->object (apply u8vector-append (cdr (vector->list vec)))))
                 (table-set! cache ID)))
           (loop (read socket))))))))

(define (connect-udp-nodes one two)
  (>> (udp-node-inbound one) ((tag-packet two) connection-packet))
  (>> (udp-node-inbound two) ((tag-packet one) connection-packet)))
