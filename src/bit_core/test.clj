;(ns bit-core.testing)
;
;(use 'clojure.test)
;(use 'bit-core.core)
;
;(def sym "BTC")
;(def tsp 123411)
;
;(deftest depthchart
;  (let [ book (routine-order-book sym tsp)
;         ask-series (price-qty-series-to-redis-server book :ask tsp )
;         bid-series (price-qty-series-to-redis-server book :bid tsp ) ]
;    
;    (is (= "ok" (store-order-book tsp))))) 


