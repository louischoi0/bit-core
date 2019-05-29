(ns bit-core.core
  (:require [clojure.data.json :as json])
  (:require [clj-http.client :as client])
  (:require [clj-time.core :as t])
  (:require [clj-time.coerce :as c])
  (:require [clj-time.format :as fm])
  (:require [monger.core :as mg])
  (:require [monger.collection :as mc])
  (:require [clojure.string :as str]))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(def ticker-history-url "https://crix-api-endpoint.upbit.com/v1/crix/candles/")
(def ticker-url "https://api.coinone.co.kr/ticker/")

(def req-dt-fmt (fm/formatter "YYYY-MM-dd HH:mm:ss"))

(defn strftime [x] (fm/unparse req-dt-fmt x))

(defn req-ticker
  [sym]
    (-> ticker-url 
         (client/get {:query-params {:currency sym} {:format "json"} {:accept :json} })
         (:body)
         (json/read-str {:key-fn keyword})))

(def unit-dict {:minutes 60 :hours (* 60 60) :day (* 60 60 24) })

(defn get-target-time
  [ target-time unit tick cnt op]
    (-> target-time
        (c/to-long)
        (op (* tick cnt (* 1000 (unit-dict (keyword unit)))))
        (c/from-long)
        strftime))

(defn get-end-time 
  [start-time unit tick cnt]
    (get-target-time start-time unit tick cnt +))

(defn get-start-time
  [end-time unit tick cnt]
    (get-target-time end-time unit tick cnt -))

(defn get-cnt-from-times
  [start-time end-time unit tick] 
    (-> (c/to-long end-time)
        (- (c/to-long start-time))
        (/ 1000 (* (unit-dict (keyword unit)) tick))))

(defn bind-unit-api-crix-endpoint
  [ unit tick ]
    (str ticker-history-url unit "/" (str tick) ))

(defn request-api-crix
  [ to-time sym unit tick cnt] 
    (let [ conv-sym (fn [x] (str "CRIX.UPBIT.KRW-" (.toUpperCase x)) ) ]
    (-> (bind-unit-api-crix-endpoint unit tick)
        (client/get {:query-params {"code" (conv-sym sym) "count" (str cnt) "to" to-time}})
        :body
        (json/read-str :key-fn keyword))))

(defn api-crix-endpoint
  [ start-time sym unit tick cnt ]
      (-> start-time 
          (get-end-time unit tick cnt)
          (request-api-crix sym unit tick cnt)))

(def test-his-req (api-crix-endpoint (t/date-time 2019 5 1) "BTC" "minutes" 10 10))
(def mongo-connection (-> (mg/connect) (mg/get-db "bit-core") ))

(defn generate-obj-id
  [ row ]
    (let [ dt (row (keyword :candleDateTime))
           sym (-> (row (keyword :code)) (str/split #"-") last ) ]
    (str sym "!" dt)))

(defn before-insert 
  [ts]
    (->> ts
    (map (fn [x] (assoc x :_id (generate-obj-id x))))))

(defn store-time-series
  [ db ts ] 
    (->>  ts 
          before-insert
          (mc/insert-batch db "bitts" ts)))

(def store (partial store-time-series mongo-connection))

(defn get-start-time-from-end-time
  [ end-time unit tick cnt ] 
    (-> end-time 
        c/to-long
        (- (* (unit-dict (keyword unit) tick cnt)))
        c/from-long
        strftime))

(defn api-crix-endpoint-from-to
  [ to-time sym unit tick cnt ]
    (request-api-crix to-time sym unit tick cnt))

(defn collect-api-crix       
  [ genesis-time start-time sym unit tick bulk-cnt ]
    (println (str "collect-api-crix start from" genesis-time "to" start-time))
    (if (t/after? start-time genesis-time)
      nil
      (do 
        (-> (api-crix-end-point-from-to start-time sym unit tick bulk-cnt)
            store)
        (collect-api-crix genesis-time (get-start-time start-time sym unit tick bulk-cnt)))))
      
;(defn req-ticker-history 
;  [sym start-time end-time unit]
;    (let [ bulk-cnt 200 ] 
;      (-> start-time
;          (get-end-time bulk-cnt unit)
;          ( 
  
    






