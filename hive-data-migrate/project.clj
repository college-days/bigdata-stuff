(defproject importdata "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.clojure/java.jdbc "0.3.5"]
                 [org.postgresql/postgresql "9.3-1100-jdbc41"]
                 [clj-time "0.8.0"]
                 [org.clojure/tools.namespace "0.2.5"]]
  :main ^:skip-aot importdata.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
