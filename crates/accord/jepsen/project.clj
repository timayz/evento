(defproject evento-accord-jepsen "0.1.0-SNAPSHOT"
  :description "Jepsen test for evento-accord: strict-serializability under faults."
  :url "https://github.com/timayz/evento"
  :license {:name "Apache-2.0"}
  :main evento-accord.core
  :jvm-opts ["-Djava.awt.headless=true"]
  :dependencies [[org.clojure/clojure "1.11.3"]
                 ;; Jepsen bundles Elle (the strict-serializability checker).
                 [jepsen "0.3.5"]
                 [clj-http "3.13.0"]
                 [cheshire "5.13.0"]]
  :repl-options {:init-ns evento-accord.core})
