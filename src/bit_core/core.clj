(ns bit-core.core
  (:require [clojure.data.json :as json])
  (:require [clj-http.client :as client])
  (:require [clj-time.core :as t])
  (:require [clj-time.coerce :as c])
  (:require [clj-time.format :as fm])
  (:require [monger.core :as mg])
  (:require [monger.collection :as mc])
  (:require [clojure.string :as str])
  (:require [clojure.tools.logging :as log])
  (:require [bit-core.utils :refer :all])
  (:require [bit-core.operation :as op]))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!"))

(def ticker-history-url "https://crix-api-endpoint.upbit.com/v1/crix/candles/")
(def ticker-url "https://api.coinone.co.kr/ticker/")

(defn req-ticker
  [sym]
    (-> ticker-url 
         (client/get {:query-params {:currency sym} {:format "json"} {:accept :json} })
         (:body)
         (json/read-str {:key-fn keyword})))

(defn bind-unit-api-crix-endpoint
  [ unit tick ]
    (str ticker-history-url unit "/" (str tick) ))

(defn request-api-crix
  [ to-time sym unit tick cnt] 
    (let [ conv-sym (fn [x] (str "CRIX.UPBIT.KRW-" (.toUpperCase x)) ) ]
    (-> (bind-unit-api-crix-endpoint unit tick)
        (client/get {:query-params {"code" (conv-sym sym) "count" (str cnt) "to" (strftime to-time)}})
        :body
        (json/read-str :key-fn keyword))))

(defn log-recv-datas
  [ form datas ]
    (->> datas
        (map :candleDateTime)
        (map (fn [x] (str/replace form #"%" x)))
        (run! (fn [x] (log/info x))))
      datas)

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
           sym (:code row) ]

    (str sym "!" dt)))

(defn conv-date-format 
  [ d ]
    (-> d 
        (str/replace #"T" " ")
        (str/split #"\+")
        first))

(defn tag-source-site
  [ site ts ]
    (map (fn [x] (assoc x :site site )) ts))

(defn before-insert-upbit
  [ts]
    (->> ts
         (map (fn [x] (assoc x :candleDateTime (conv-date-format (:candleDateTime x)))))
         (map (fn [x] (assoc x :code (-> x :code (str/split #"-") last))))
         (tag-source-site "upbit")
         (map (fn [x] (assoc x :_id (generate-obj-id x))))))

(defn print-recur
  [x]
    (println x)
    x)

(defn log-before-store
  [ ts ] 
    (log/info ts)
    ts)

(defn store-time-series
  [ db ts ] 
    (let [ cts (->>  ts before-insert-upbit) ]
      (mc/insert-batch db "bitts" cts)
        (-> cts
            last 
            :candleDateTime
            timefstr)))

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
  [ genesis-time sym unit tick bulk-cnt start-time ]
    (let [ routine (partial collect-api-crix  genesis-time sym unit tick bulk-cnt) 
           get-time (fn [x] (get-start-time x unit tick bulk-cnt)) 
           req-routine (fn [x] (api-crix-endpoint-from-to x sym unit tick bulk-cnt)) ]

      (log/info (str "collect-api-crix " sym  " start from " (get-time start-time) " to " start-time))

      (if (t/after? genesis-time start-time)
        nil
        (do 
          (->>  start-time 
                req-routine  
                store
                print-recur
                routine)))))

(defn get-min-timestamp
  [ sym unit ]
    (op/get-min-timestamp sym unit mongo-connection "bitts"))

(get-min-timestamp "BTC" 10)

(def get-start-timestamp get-start-time)
(def get-end-timestamp get-end-time)

(def first-req-time (t/date-time 2019 5 30))

(defn collect-api-crix-a
  [ sym unit tick cnt rept ]

    (let [  db-start-time (get-min-timestamp sym tick) 
            start-time (if (nil? db-start-time) first-req-time db-start-time)
            genesis-time (get-start-timestamp start-time unit tick (* cnt rept )) ]

      (collect-api-crix genesis-time sym unit tick cnt start-time))
    "ok")

;(get-min-timestamp "BTC" 10) 
;(collect-api-crix-a "BTC" "minutes" 10 10 10)

(def gen-time (t/date-time 2019 1 28))
(def target-time (t/date-time 2019 5 30))

(def req-ticks [1 10 15 20 30])
(def req-ticks [10])

(def req-coins ["BTC" "EOS" "BSV" "BCH" "ETH" "BTT" "COSM" "BTG" "ADA" "ATOM" "TRX" "NPXS"])
(def bulk-cnt 100)
(def recur-max-cnt 10)

(defn cartesian
  [ a-set b-set ]
    (for [a a-set b b-set] [ a b ]))

(cartesian req-coins req-ticks)

(defn collect-crix-site 
  [ coins ticks ] 
  (let [ task-set (cartesian coins ticks) ]
    (map (fn [x] (collect-api-crix-a (first x) "minutes" (last x) bulk-cnt recur-max-cnt)) task-set)))

(collect-crix-site req-coins req-ticks)

   ;(map  task-set ))))

;(def t (op/get-min-timestamp mongo-connection "bitts" "BTC" ))
;(pprint t)

;(def target-time (get-min-timestamp-in-db "BTC"))

;(println target-time)

;(println target-time)

;(collect-api-crix gen-time "BTC" "minutes" 10 10 target-time)


;(defn req-ticker-history 
;  [sym start-time end-time unit]
;    (let [ bulk-cnt 200 ] 
;      (-> start-time
;          (get-end-time bulk-cnt unit)
;          ( 
  
