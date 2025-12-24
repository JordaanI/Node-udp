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

(define-type udp-node id: 26A52C1A-CFC5-4526-A10D-2ED793974E41 ID socket connections channels data)

(define (new-udp-node address port
                      #!key
                      (ID (generate-guid))
                      (in-transducer $identity)
                      (out-transducer $identity)
                      (data (make-table)))
  (let* ((udp-node (make-udp-node
                    ID
                    (open-udp (list local-address: (string-append address ":" port)))
                    (make-table)
                    (make-table)
                    data))
         (in-channel (create-channel transducer: ($compose
                                                  ($filter packet?)
                                                  in-transducer)))
         (out-channel (create-channel transducer: ($compose
                                                   ($filter packet?)
                                                   ($map (tag-packet udp-node))
                                                   out-transducer))))
    (udp-node-channels-set! udp-node (list->table `((in-channel . ,in-channel)
                                                    (out-channel . ,out-channel))))
    (pp udp-node)
    udp-node))

(define (udp-node-in-channel udp-node)
  (table-ref (udp-node-channels udp-node) 'in-channel))

(define (udp-node-out-channel udp-node)
  (table-ref (udp-node-channels udp-node) 'out-channel))

(define (start-udp-node udp-node)
  (start-reader udp-node)
  (channel-consumer (udp-node-out-channel udp-node)
                    (%processor (batch-send udp-node)))
  (println "Udp-Node started: " (udp-node-ID udp-node))
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
                 (>> (udp-node-in-channel udp-node) (u8vector->object (apply u8vector-append (cdr (vector->list vec)))))
                 (table-set! cache ID)))

           (loop (read socket))))))))

(define (consume-input udp-node #!key registration-ext (action identity))
  (let ((in-channel (udp-node-in-channel udp-node)))
    (channel-consumer
     in-channel
     (%processor (lambda (packet)
                   (let ((ID (packet-ID packet))
                         (connections (udp-node-connections udp-node)))
                     (if (not (table-ref connections ID #f))
                         (let ((ret (packet-copy packet)))
                           (table-set! connections ID (packet-source packet))
                           (packet-destination-set! ret (packet-source packet))
                           (packet-args-set! ret (list #f))
                           (if registration-ext (packet-sub-set! ret (list registration-ext)))
                           (if (car (packet-args packet)) (>> (udp-node-out-channel udp-node) ret))
                           (for-each (lambda (datum) (>> in-channel datum)) (packet-sub packet))
                           (display (string-append "Registered: " ID " to " (udp-node-ID udp-node)))
                           (newline)))
                     (if (not (eq? '_connect_ (packet-function packet)))
                         (action packet))))))))

(define (connect-udp-node-to udp-node host)
  (let ((socket (udp-node-socket udp-node)))
    (udp-destination-set! (table-ref host 'address) (table-ref host 'port-number) socket)
    (for-each (lambda (datum)
                (write datum socket))
              (pack-packet ((tag-packet udp-node) (new-packet '_connect_ (list #t))) BATCH_SEND_SIZE))))

(define (connect-udp-nodes s c)
  (>> (udp-node-out-channel s) ((set-packet-destination c) (new-packet '_connect_ (list)))))

(define (set-packet-destination udp-node packet destination-ID)
  (packet-destination-set! packet (table-ref (udp-node-connections udp-node) destination-ID))
  packet)

(define (send-packet packet udp-node destination-ID)
  (>> (udp-node-out-channel udp-node)
      (set-packet-destination udp-node packet destination-ID)))
