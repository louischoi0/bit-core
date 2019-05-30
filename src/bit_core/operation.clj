(ns bit-core.operation
  (:require [monger.core :as mg]
            [monger.collection :as mc]
            [monger.operators :refer :all]
            [bit-core.utils :refer :all]))

(defn get-min-timestamp
  [ sym unit db collection ]
    (-> (mc/aggregate db collection 
                  [ { $match {$and [ {:unit unit} {:code sym } ] } } { $group  {:_id "$code" :min-time {$min "$candleDateTime"} } } ] :cursor {:batch-size 0})
        first
        :min-time
        timefstr))

