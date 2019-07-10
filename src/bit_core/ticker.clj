(ns bit-core.ticker (:require [clojure.data.json :as json])
  (:require [clj-http.client :as client])
  (:require [clj-time.core :as t])
  (:require [clj-time.coerce :as c])
  (:require [taoensso.carmine :as car :refer (wcar)])
  (:require [clj-time.format :as fm])
  (:require [clj-time.local :as l])

  (:require [monger.core :as mg])
  (:require [monger.collection :as mc])

  (:require [clojure.tools.cli :refer [parse-opts]])

  (:require [clojure.string :as str])
  (:require [clojure.tools.logging :as log])
  (:require [bit-core.utils :refer :all])
  (:require [bit-core.core :as cc])
  (:require [bit-core.redis :refer :all])
  (:require [clojure.data.csv :as csv])
  (:require [bit-core.operation :as op]))

(use 'overtone.at-at)
(use 'bit-core.core)
(use 'bit-core.utils)

(def coins [ "BTC" "ETH" "XRP" "BCH" "EOS" "BSV" ] )
(def coinss [ "btc" "eth" "xrp" "bch" "eos" "bsv" ] )

(def flush-inc-cnt (atom 0))
(def flushed-inc-cnt (atom 0))

(defn inc-atomic
  [ acnt ] 
    (reset! acnt (inc @acnt)))

(defn inca
  [ ]
    (inc-atomic flush-inc-cnt))

(defn acca
  [ atm v ]
    (reset! atm (+ @atm v)))

(defn --insert-many 
  [ db table data ]
    (cc/log "Insert Data")
    (mc/insert-batch db table data))

(defn -insert-many
  [ db table ]
    (partial --insert-many db table))

(defn flush-interface 
  [ ]
    (atom {:data (atom (vector)) :status (atom 0)})) 

(def data-to-flush (flush-interface))

;mongo-connection is from bit-core.core
(def insert-many (-insert-many mongo-connection "candles"))

(defn write-flush
  [ data ]
    (insert-many data))

(defn op2f
  [ f x y ]
    (f y x))

(defn -req-ticker
  [ url sym ] 
    (->> url  
        (op2f client/get {:query-params {:currency sym} {:format "json"} {:accept :json} })
        :body
        json/read-str
        (reduce (fn [x,y] (assoc x (keyword (key y)) (val y))) {})))

(defn req-ticker
  [ sym ]
    (-req-ticker cc/ticker-url sym))

(defn -store-field
  [ source data field ]
    (let [ sym (-> data :currency) 
           skey (-> sym (str "-" field)) ]
      (cc/log (str "Store Ticker " field "Data " sym ))
      (wcar* (car/lpush (str source "-" skey) ((keyword field) data)))
        "OK"))
    
(defn store-field 
  [ data field ]
    (-store-field "coinone" data field))

(defn -req-ticker-and-store 
  [ source sym ] 
    (let [ data (req-ticker sym)
           targets ["volume" "last"] ] 
     (cc/log (str "Request Ticker " sym ))
     (-store-field data source "volume")
     (-store-field data source "last") 
      data))

(defn req-ticker-and-store 
  [ sym ]
    (-req-ticker-and-store "coinone" sym))

(defn store-timestamp 
  [ series-key ] 
  (let [ timestamp (-> (l/local-now) c/to-long) ]
    (cc/log "Store timestamp series.")
    (wcar* (car/lpush series-key timestamp))))

(defn call-or-retry
  [ callback cond ms ] 
    (if (cond)
      (callback)
      (do 
        (cc/log "Wait and Retry")
        (Thread/sleep ms)
        (call-or-retry callback cond ms))))

(defn flush-subroutine
  [ fi cnt ]
    (lock- fi 1)
    (insert-many (getv fi))
    (reset! (getva fi) (subvec (getv fi) cnt))
    (acca flushed-inc-cnt cnt) 
    (unlock- fi))

(defn -flush-routine 
  [ fi limit ms ]
    (let [ cond (fn [] (= (gets fi) 0)) 
           cnt (count (getv fi))
           callback (fn [] (flush-subroutine fi cnt)) ] 
      (if (> cnt limit)
        (do 
          (call-or-retry callback cond ms))
        nil)))

(defn collect-data-to-flush
  [ data cvec ]
    (inca)
    (reset! cvec (conj @cvec data)))

(defn -ticker-routine-by-sym
  [ fi sym ]
    (let [ data (req-ticker-and-store sym) ]
      (lock- fi 1)
      (collect-data-to-flush data (getva fi))
      (unlock- fi))) 

(defn ticker-routine-by-sym
  [ sym ]
    (-ticker-routine-by-sym data-to-flush sym))

(defn schedule-ticker 
  [ pool  sym interval ]
    (every interval #(ticker-routine-by-sym sym) pool))

(defn schedule-time-keeper 
  [ pool interval ]
    (every interval #(store-timestamp "timestamp-series")  pool ))

(defn cartesian-coin-field
  [ coins fields ]
    (->> (map (fn [x] (map (fn [y] (str x "-" y)) fields)) coins)
         (reduce concat)))

(defn do-all-series 
  [ f ] 
    (let [ all-series-ids (cartesian-coin-field coinss ["volume" "last"]) ]
      (->> (wcar* (mapv f all-series-ids))
           (map (fn [x y] { (keyword x) y }) all-series-ids)
           (reduce conj))))

(defn main-ticker 
  [ interval coins ] 
    (let [ ticker-pool (mk-pool)
           timestamp-pool (mk-pool) 
           flush-pool (mk-pool) ]
      (schedule-time-keeper timestamp-pool interval)
      (every 5000 #(-flush-routine data-to-flush 30 400) flush-pool)
      (map (fn [x] (schedule-ticker ticker-pool x interval)) coins)))

(defn slice-redis-series
  [ cnt-to-cut ]
    (do-all-series #(car/ltrim 0 cnt-to-cut)))

(defn main**
  []
    (main-ticker 3000 coins))

(defn clear-
  []
    (wcar* (car/del "timestamp-series"))
    (do-all-series #(car/del %1)))

(defn len-
  []
    (let [ stamp-len {:timestamp-series (wcar* (car/llen "timestamp-series")) }
           values-len (do-all-series #(car/llen %1))  ]
      (conj stamp-len values-len)))

(def datas (do-all-series #(car/rpop %1)))

;(-flush-routine data-to-flush 10 100)
;(-> data-to-flush getv count)
;(-> @flush-inc-cnt)
;(-> @flushed-inc-cnt)
;(len-)

;(clear-)
;(len-)
;(do-all-series #(car/rpop %1)) 

