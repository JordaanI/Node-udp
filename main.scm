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

(define-type node id: 26A52C1A-CFC5-4526-A10D-2ED793974E41 ID socket connections channels running?)

(define (new-node address port
                  #!key (ID (random-integer 100000))
                  (in-transducer $identity)
                  (out-transducer $identity))
  (let* ((node (make-node
                ID
                (open-udp (list local-address: (string-append address ":" port)))
                (make-table)
                (make-table)
                #f))
         (in-channel (create-channel transducer: ($compose
                                                  ($filter packet?)
                                                  in-transducer)))
         (out-channel (create-channel transducer: ($compose
                                                   ($map (tag-packet node))
                                                   out-transducer))))
    (node-channels-set! node (list->table `((in-channel . ,in-channel)
                                            (out-channel . ,out-channel))))
    (pp node)
    node))

(define (node-in-channel node)
  (table-ref (node-channels node) 'in-channel))

(define (node-out-channel node)
  (table-ref (node-channels node) 'out-channel))

(define (start-node node)
  (start-reader node)
  (channel-consumer (node-out-channel node)
                    (%processor (batch-send node)))
  (node-running?-set! node #t)
  (println "Node started: " (node-ID node))
  node)

(define (batch-send node)
  (let ((batch-size BATCH_SEND_SIZE)
        (socket (node-socket node)))
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

(define (start-reader node)
  (let ((cache (make-table init: #f))
        (socket (node-socket node)))
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
                 (>> (node-in-channel node) (u8vector->object (apply u8vector-append (cdr (vector->list vec)))))
                 (table-set! cache ID)))

           (loop (read socket))))))))

(define (consume-input node #!key registration-ext (action identity))
  (let ((in-channel (node-in-channel node)))
    (channel-consumer
     in-channel
     (%processor (lambda (packet)
                   (let ((ID (packet-ID packet))
                         (connections (node-connections node)))
                     (if (not (table-ref connections ID #f))
                         (let ((ret (packet-copy packet)))
                           (table-set! connections ID (packet-source packet))
                           (packet-destination-set! ret (packet-source packet))
                           (packet-args-set! ret (list #f))
                           (if registration-ext (packet-sub-set! ret (list registration-ext)))
                           (if (car (packet-args packet)) (>> (node-out-channel node) ret))
                           (for-each (lambda (datum) (>> in-channel datum)) (packet-sub packet))
                           (display (string-append "Registered: " ID " to " (node-ID node)))
                           (newline)))
                     (if (not (eq? '_connect_ (packet-function packet)))
                         (action packet))))))))

(define (connect-node-to node host)
  (let ((socket (node-socket node)))
    (udp-destination-set! (table-ref host 'address) (table-ref host 'port-number) socket)
    (for-each (lambda (datum)
                (write datum socket))
              (pack-packet ((tag-packet node) (new-packet '_connect_ (list #t))) BATCH_SEND_SIZE))))

(define (connect-nodes s c)
  (>> (node-out-channel s) ((set-packet-destination c) (new-packet '_connect_ (list)))))

(define (set-packet-destination node packet destination-ID)
  (packet-destination-set! packet (table-ref (node-connections node) destination-ID))
  packet)

(define (send-packet packet node destination-ID)
  (>> (node-out-channel node)
      (set-packet-destination node packet destination-ID)))
