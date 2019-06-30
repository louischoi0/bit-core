(ns bit-core.utils
  (:require [clj-time.core :as t])
  (:require [clj-time.local :as l])
  (:require [clj-time.coerce :as c])
  (:require [clj-time.format :as fm]))

(def unit-dict {:minutes 60 :hours (* 60 60) :day (* 60 60 24) })

(def req-dt-fmt (fm/formatter "YYYY-MM-dd HH:mm:ss"))

(defn strftime [x] (fm/unparse req-dt-fmt x))
(defn timefstr [x] (if (nil? x) nil (fm/parse req-dt-fmt x)))


(defn conv-time-zone-seoul
  [ date-time ]
    (t/from-time-zone date-time (t/time-zone-for-offset -16)))

(defn get-target-time
  [ target-time unit tick cnt op]
    (-> target-time
        (c/to-long)
        (op (* tick cnt (* 1000 (unit-dict (keyword unit)))))
        (c/from-long)))

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

(defn getva
  [ fi ] 
    (:data @fi))

(defn getsa
  [ fi ]
    (:status @fi))

(defn getv
  [ fi ]
    @(getva fi))

(defn gets
  [ fi ]
    @(getsa fi))

(defn lock-
  [ fi lv ]
    (reset! (getsa fi) lv))

(defn unlock-
  [ fi ]
    (reset! (getsa fi) 0))


