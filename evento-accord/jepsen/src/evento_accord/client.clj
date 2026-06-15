(ns evento-accord.client
  "Jepsen client: drives one node's HTTP /txn endpoint with Elle list-append
  transactions and maps the response onto Jepsen op outcomes.

  The critical mapping is the failure mode:

    * HTTP 200  -> :ok   (committed; reads filled in)
    * HTTP 409  -> :fail (optimistic-concurrency conflict; definitely NOT applied)
    * anything else / timeout / connection error -> :info (INDETERMINATE)

  An indeterminate append may still have committed on the cluster, so it must be
  :info, never :fail. Mislabeling it :fail would tell Elle the write is absent and
  manufacture phantom anomalies."
  (:require [cheshire.core :as json]
            [clj-http.client :as http]
            [jepsen.client :as client]))

(def ^:private http-port 8080)

(defn- url [node]
  (str "http://" node ":" http-port "/txn"))

(defn- mop->json
  "Elle micro-op -> wire op. [:append k v] -> [\"append\" k v];
  [:r k nil] -> [\"r\" k nil]."
  [[f k v]]
  [(name f) k v])

(defn- merge-mop
  "Wire response op -> Elle micro-op, reusing the ORIGINAL op's key (the wire echoes
  keys as strings; Elle keys are integers). Reads gain their observed value vector."
  [orig [f _k v]]
  (if (= "r" f)
    [:r (second orig) (vec v)]
    orig))

(defrecord Client [node]
  client/Client
  (open! [this _test node]
    (assoc this :node node))

  (setup! [_this _test])

  (invoke! [this _test op]
    (let [txn  (:value op)
          body (json/generate-string {:ops (mapv mop->json txn)})
          resp (try
                 (http/post (url (:node this))
                            {:body             body
                             :content-type     :json
                             :accept           :json
                             :socket-timeout   5000
                             :connection-timeout 5000
                             :throw-exceptions false})
                 (catch java.net.ConnectException _ {:status -1})
                 (catch java.net.SocketTimeoutException _ {:status -1})
                 (catch org.apache.http.conn.ConnectTimeoutException _ {:status -1})
                 (catch java.io.IOException _ {:status -1})
                 (catch Exception _ {:status -2}))]
      (case (long (:status resp))
        200 (let [parsed (json/parse-string (:body resp) true)
                  out    (mapv merge-mop txn (:ops parsed))]
              (assoc op :type :ok :value out))
        409 (assoc op :type :fail :error :conflict)
        ;; -1/-2/500/503/... : indeterminate
        (assoc op :type :info :error (str "status " (:status resp))))))

  (teardown! [_this _test])

  (close! [_this _test]))

(defn client
  "A fresh Jepsen client (node is bound per-thread in open!)."
  []
  (->Client nil))
