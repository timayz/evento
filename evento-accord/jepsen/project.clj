(defproject evento-accord-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen test for evento-accord: strict-serializability under faults."
  :url "https://github.com/timayz/evento"
  :license {:name "Apache-2.0"}
  :main evento-accord.core
  :jvm-opts ["-Djava.awt.headless=true"]
  :dependencies [[org.clojure/clojure "1.12.5"]
                 ;; Jepsen bundles Elle (the strict-serializability checker).
                 [jepsen "0.3.11"]
                 [clj-http "3.13.1"]
                 [cheshire "6.2.0"]]
  :repl-options {:init-ns evento-accord.core})
