(ns double-thread.core
    (:gen-class)
    (import [clojure.lang PersistentVector])
    (import [java.lang String Class])
    (:require [clojure.core.async :refer [sub <! put! pub go-loop chan]]))

;; DEFENITIONS

;; map гарантирует количество аргументов
(defn get-node-id [& args] 
   (-> (fn [obj] 
           (let [cl (class obj)] 
             (if (= Class cl) obj cl)))
       (map args)))

(defmulti thread-handler get-node-id)

(def collector (atom {}))

;; TODO реализация коллектора
(defmethod thread-handler :default [& args]
  (prn (str "Errorring for args: " (apply get-node-id args))))

(def incoming-chan (chan 4096))

(let [global-chan (pub incoming-chan :type)
      stream-queue (sub global-chan :stream (chan 2048))
      transition-queue (sub global-chan :transition (chan 2048))]
  (go-loop [stream-message (<! stream-queue)]
           (apply thread-handler (:body stream-message))
           (recur (<! stream-queue))) 
  (go-loop [transition-message (<! transition-queue)]
           (apply thread-handler (:body transition-message))
           (recur (<! transition-queue))))
      
(defrecord Request [callers])

(defrecord Confirm [callers])

;; Обработчики начальной ноды? 
(defmethod thread-handler [Request] [r])
(defmethod thread-handler [Confirm] [c])
(defmethod thread-handler [Error] [e])

(defrecord Message [type body])

(defn send-message [chan-key body]
  (put! incoming-chan (Message. chan-key body)))

;; API???

(defn request [state data] ;; (расширить на несколько аргументов ???)
  (send-message :stream [(Request. state) data]))

(defn confirm [state data] 
  (send-message :transition [(Confirm. state) data]))

(defn error [state message]
  (send-message :transition [(Error. message) state]))

; берем вектор ["a" "b" "c"] и инициируем цепочку
; Node-Handler PersistentVector -> Node-Handler String
; в обработчике вызывается ошибка и отправляется соответствующее сообщение

(defmethod thread-handler [Request String] [{cs :callers} _]
  (prn "String (Captain)")
  (confirm :ok (first cs)))

(defmethod thread-handler [Confirm String] [& args])

(defmethod thread-handler [Error String] [& args])

(defmethod thread-handler [Request PersistentVector] [{cs :callers} v & args]
  (doall (map #(request (cons PersistentVector cs) %) v)))

;; TODO обработчик инициатора???
(defmethod thread-handler [Confirm PersistentVector] [& args]
  (prn "Сообщение обработано")
  #_(confirm :ok PersistentVector))

(defmethod thread-handler [Error PersistentVector] [& args]
  (prn "Опять хвост виляет собакой."))

(defn run-threads []
  ;; В запросе можно хранить стек вызовов 
  ;; пройденный маршрут а также id потока 
  ;; (чтобы можно было подтверждать отправку в нескольких потоках???)
  ;; Возможно request и confirm потребуется скрыть
  ;; для чего потребуется инициатор потока (воронки из Storm)
  #_(request [] ["a" "b" "c"])
  (doall (map #(do (Thread/sleep 500) (request [] %)) (repeat ["a" "b" "c"]))))

(defn -main [& args]
  (let  [f (future (run-threads))]
    (Thread/sleep 2000)
    (eval '(do
             (in-ns 'double-thread.core)
             (defmethod thread-handler [Request String] [{cs :callers} _]
               (error (first cs) "Error string"))
             (in-ns 'user) ;; проблема вернуться в изначальный ns
             ))
    (Thread/sleep 2000)
    (future-cancel f)))

