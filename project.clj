(defproject bit-core "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}

  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.4.1"]
                 [org.clojure/data.csv "0.1.4"]
                 [overtone/at-at "1.2.0"]
                 [com.taoensso/carmine "2.19.1"]
  
                 [org.clojure/tools.cli "0.4.2"] 

                 [clj-http "3.10.0"]
                 [org.clojure/tools.nrepl "0.2.13"]
                 [clj-time "0.15.0"]
                 [com.novemberain/monger "3.1.0"]
                 [org.clojure/data.json "0.2.6"]]
  
  :profiles {:uberjar {:aot :all}}

  :main bit-core.ticker)

