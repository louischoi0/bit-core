(ns bit-core.core
  (:require [clojure.data.json :as json])
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
  (:require [bit-core.redis :refer :all])
  (:require [clojure.data.csv :as csv])
  (:require [bit-core.operation :as op]))

(use 'bit-core.utils)

; To Replace file log or stream log.
(defn logger
  [ lv msg ]
    (let [ timestamp (-> (l/local-now) c/to-long) 
           surfix (str "[INFO] " (-> timestamp c/from-long conv-time-zone-seoul) " : ") ]
      (println (str surfix msg))))

(defn no-op
  [x] 
  x)

(def loglv 1)
(def log (partial logger loglv))

(defn print-recur
  [x]
    (println x)
      x)

(def ticker-order-book "https://api.coinone.co.kr/orderbook/")
(def ticker-history-url "https://crix-api-endpoint.upbit.com/v1/crix/candles/")
(def ticker-url "https://api.coinone.co.kr/ticker/")

(defn nil-to-false
  [ v ]
    (if (nil? v) false v))

(defn isin?
  [ v arr ]
  (->> arr
       (some (fn [x] (= x v)))
        nil-to-false))

(defn nmap 
  [ s f ]
    (map f s))

(defn op2-reverse 
  [ x y f ]
    (f y x))

(defn nfilter
  [ x f ] (filter f x)) (defn entry-array-vfk [ ena k ] (-> ena
        (nfilter (fn [x] (= (key x) k )))
        first))

(defn get-with-sym
  [ url sym ]
    (client/get url {:query-params {:currency sym } {:format "json"} {:accept :json}}))

(defn req-order-book
  [ sym ]

  (let [ what-i-need '("ask" "bid" "currency")  ]
    (-> ticker-order-book
        (get-with-sym sym)
        :body
        json/read-str   
        (nfilter (fn [x] (-> x key (isin? what-i-need)))))))

(defn routine-order-book
  [ sym request-time ]
    (let [ book (-> sym req-order-book) 
           currency (entry-array-vfk book "currency")

           ask (->> (entry-array-vfk book "ask") val (sort-by :price ))
           bid (->> (entry-array-vfk book "bid") val (sort-by :price )) ]

    {:request-time request-time :ask ask :bid bid :currency (last currency)}))

(defn rpush-in-server
  [ k v ]
    (log (str k " : " v))
    (wcar* (car/rpush k v))) 

(defn print-recur
  [x]
    (println x)
    x)

(defn fkernel
  [ v1 v2 ]
    (->> v2
         (zipmap v1) 
         (mapcat no-op)))

(defn price-qty-series-to-redis-server
  [ book what req-time ]

    (let [ f  (partial rpush-in-server req-time)
           price (-> book what (nmap first) (nmap last))
           qty (-> book what (nmap last) (nmap last)) ]

      (fkernel price qty)))

(defn order-book-request-logger 
  [ sym stamp ]
    (log (str "Orderbook requested " sym " " (str stamp))))

(defn store-order-book
  [ book stamp ]
    (let [ ask-series  (price-qty-series-to-redis-server book :ask stamp)
           bid-series  (price-qty-series-to-redis-server book :bid stamp) ]
      (wcar* 
        (car/rpush stamp (book :currency))
        (mapv #(car/rpush %1 %2) (repeat stamp) ask-series)
        (mapv #(car/rpush %1 %2) (repeat stamp) bid-series)
        (car/rpush stamp "done"))
      
     "ok"))

(defn order-book-request-handler 
  [ sym stamp ]
    (let [ book (routine-order-book sym stamp) ]
      (order-book-request-logger sym stamp)
      (store-order-book book stamp)))

;(order-book-request-handler "BTC" 12300)

;
;(def book (routine-order-book "BTC" 1236))
;(-> book :ask)
;(-> book :currency)
;
;(order-book-request-handler "BTC" 1253)
;(aa "BTC" 1112)
;(def t [ 11 12 13 14] )
;(wcar* (mapv #(car/rpush %1 %2) (repeat 11111) t))
;
;(-> book :ask (nmap first) (nmap last) first)
;
;(-> book (price-qty-series-to-redis-server :ask 1239)) 


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

(def mongo-connection (-> (mg/connect) (mg/get-db "bit-core") ))

(defn generate-obj-id
  [ row unit ]
    (let [ dt (row (keyword :candleDateTime))
           sym (:code row) ]

    (str sym "!" unit "!" dt)))

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
  [ ts unit ]
    (->> ts
         (map (fn [x] (assoc x :candleDateTime (conv-date-format (:candleDateTime x)))))
         (map (fn [x] (assoc x :code (-> x :code (str/split #"-") last))))
         (tag-source-site "upbit")
         (map (fn [x] (assoc x :_id (generate-obj-id x unit))))))


(defn log-price-row
  [row]
    (log/info (str "Insert " (:code row) " AT " (:candleDateTime row) " "  (:unit row) "U"))
    row)

(defn store-time-series
  [ db ts unit ] 
    (let [ cts (-> ts (before-insert-upbit unit)) ]
      (mc/insert-batch db "bitts" cts)
        (->> cts
            (map log-price-row)
            last 
            :candleDateTime
            timefstr)))

(defn store
  [ ts ]
    (store-time-series mongo-connection ts 10))    

;(def store (partial store-time-series mongo-connection))

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
           req-routine (fn [x] (api-crix-endpoint-from-to x sym unit tick bulk-cnt)) 
           store-lambda (fn [x] (store-time-series mongo-connection x tick)) ]

      (log/info (str "collect-api-crix " sym  " start from " (get-time start-time) " to " start-time))

      (if (t/after? genesis-time start-time)
        nil
        (do 
          (->> start-time 
              req-routine  
              store-lambda 
              routine)))))

(defn get-min-timestamp
  [ sym unit ]
    (op/get-min-timestamp sym unit mongo-connection "bitts"))

(defn get-max-timestamp
  [ sym unit ]
    (op/get-max-timestamp sym unit mongo-connection "bitts"))

;(get-max-timestamp "BTC" 10)
;

(def get-start-timestamp get-start-time)
(def get-end-timestamp get-end-time)
;
(def first-req-time (t/date-time 2019 5 30))

(defn collect-api-crix-a
  [ sym unit tick cnt rept ]

    (let [  db-start-time (get-min-timestamp sym tick) 
            start-time (if (nil? db-start-time) first-req-time db-start-time)
            genesis-time (get-start-timestamp start-time unit tick (* cnt rept )) ]
      (collect-api-crix genesis-time sym unit tick cnt start-time))
    "ok")

;(collect-api-crix-a "XRP" "minutes" 10 10 10)

(def gen-time (t/date-time 2019 1 1))
(def target-time (t/date-time 2019 6 10))
;
(def req-ticks [1 10 15 20 30])
(def req-ticks [1 10])
;
(def req-coins ["XRP" "BTC" "EOS" "BSV" "BCH" "ETH" "BTT" "COSM" "BTG" "ADA" "ATOM" "TRX" "NPXS"])
(def req-coins-s [])
;
(def bulk-cnt 100)
(def recur-max-cnt 10)
;
(def for-iter-max-cnt 10000)

(defn cartesian
  [ a-set b-set ]
    (for [a a-set b b-set] [ a b ]))

;(cartesian req-coins req-ticks)

(defn collect-crix-site 
  [ coins ticks ] 
    (let [ task-set (cartesian coins ticks) ]
      (for [ x (range for-iter-max-cnt) ]
        (map (fn [x] (collect-api-crix-a (first x) "minutes" (last x) bulk-cnt recur-max-cnt)) task-set))))


(defn surfix?
  [ x ]
    (-> x
        first
        (= (first "-"))))

(defn not-to
  [ f ]
    (fn [x] (not (f x)))) 

(defn opt-parse
  [ args ]
    (let [ op (filter surfix? args) op-v (filter (not-to surfix?) args) ]
      (->>  op-v
            (map (fn [x y] {(keyword x) y}) op)
            (reduce conj))))

(defn get-opt
  [ options k ]
    (-> options
        k))

;
;(defn -main
;  [ & args ]
;    (let [ options (opt-parse args) ]
;      (if (-> options (get-opt :-op) (= "orderbook"))
;        (do (let [ sym (get-opt options :-s) request-time (get-opt options :-rqt) ]
;          (order-book-request-handler sym request-time))))))

;(map  task-set ))))
;(pprint t)
;(def target-time (get-min-timestamp-in-db "BTC"))
;(println target-time)
;(println target-time)

;(defn req-ticker-history 
;  [sym start-time end-time unit]
;    (let [ bulk-cnt 200 ] 
;      (-> start-time
;          (get-end-time bulk-cnt unit)
;          ( 
  
