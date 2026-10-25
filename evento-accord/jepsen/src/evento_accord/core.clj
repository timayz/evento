(ns evento-accord.core
  "Jepsen test for evento-accord.

  Workload (deliberately matched to what evento-accord *promises*):

    * WRITE transactions append a unique value to 1..N **distinct** keys in a
      single `executor.write(Vec<Event>)` — one atomic, strictly-serializable,
      multi-aggregate conditional append. This is the cross-key consensus edge
      Elle uses to tie keys together.
    * READ transactions read **one** key's whole list. Reads are served from each
      replica's local backend per aggregate and are not coordinated across keys,
      so we never request a multi-key snapshot evento-accord does not promise.
      (A single multi-key *write* is still atomic; a multi-key *read* is not.)

  Elle (bundled with Jepsen) checks the recorded history for strict-serializability
  cycles. A WRITE-cycle anomaly (G0/G1/lost-update among append txns) would be a
  consensus safety bug; a read-related anomaly under partition would say reads off
  a partitioned replica are not linearizable — both are real findings worth having.

  Faults (--faults, default partition,kill,pause): `jepsen.nemesis.combined` bundles
  network partitions, process kill (SIGKILL + restart, exercising journal recovery),
  and process pause (SIGSTOP/SIGCONT) with a unified schedule. `journal` (off by
  default, #340) loses a node's journal, its whole disk, or rolls its journal back
  to an older copy, one node at a time; the node comes back joining. Clock skew
  (:clock) is *available* but excluded by default — it bumps the kernel's wall clock,
  which a shared-kernel Docker cluster cannot isolate from the host, so it belongs on
  real VMs (and needs a C compiler on the node image for jepsen's time helper).

  Before the final reads, the test waits until every node's `/health` reads `ready`
  (`:await-ready`, which reports the nodes that never got back)."
  (:require [clojure.string :as str]
            [clj-http.client :as http]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [control :as c]
                    [generator :as gen]
                    [nemesis :as n]
                    [tests :as tests]]
            [jepsen.nemesis.combined :as nc]
            [jepsen.os :as os]
            [jepsen.tests.cycle.append :as append]
            [evento-accord.client :as ec-client]
            [evento-accord.db :as ec-db]))

(def key-count 8)
(def max-append-keys 3)

;; Globally-unique appended values (Elle requires per-key uniqueness; global
;; monotonicity is the simplest way to guarantee it).
(def value-counter (atom 0))

(defn- gen-op
  "One sound transaction: a single-key read, or an atomic multi-key append."
  [_ _]
  (if (< (rand) 0.5)
    {:type :invoke, :f :txn, :value [[:r (rand-int key-count) nil]]}
    (let [n  (inc (rand-int max-append-keys))
          ks (take n (shuffle (range key-count)))]
      {:type  :invoke
       :f     :txn
       :value (mapv (fn [k] [:append k (swap! value-counter inc)]) ks)})))

(defn- ready?
  "Whether `node`'s `/health` reads `ready`, asked with a `timeout-ms` timeout."
  [node timeout-ms]
  (try
    (let [resp (http/get (str "http://" node ":" ec-db/http-port "/health")
                         {:socket-timeout     timeout-ms
                          :connection-timeout timeout-ms
                          :throw-exceptions   false})]
      (= "ready" (str/trim (str (:body resp)))))
    (catch Exception _ false)))

(defn- not-ready
  "The nodes whose `/health` does not read `ready`, asked all at once."
  [test timeout-ms]
  (let [nodes (vec (:nodes test))]
    (vec (keep identity (pmap (fn [node] (when-not (ready? node timeout-ms) node)) nodes)))))

(defn- await-ready
  "Waits, at most `limit-ms`, until every node's `/health` reads `ready`. Returns
  nil once they all do, or the nodes that never did."
  [test limit-ms]
  (let [deadline (+ (System/currentTimeMillis) limit-ms)]
    (loop []
      (let [waiting (not-ready test 2000)]
        (cond (empty? waiting)                        nil
              (> (System/currentTimeMillis) deadline) waiting
              :else                                   (do (Thread/sleep 1000) (recur)))))))

(defn- ready-package
  "Answers `:await-ready`: waits, at most 180 s, until every node's `/health`
  reads `ready`, and records the nodes that never did."
  []
  {:nemesis (reify
              n/Reflection
              (fs [_] #{:await-ready})
              n/Nemesis
              (setup! [this _test] this)
              (invoke! [_ test op]
                (if-let [waiting (await-ready test 180000)]
                  (assoc op :value {:never-ready waiting})
                  (assoc op :value :all-ready)))
              (teardown! [_ _test]))})

(defn- journal-fault!
  "Runs the journal fault `op` names on a node, `copied` holding the node whose
  journal was copied aside. Returns the node it hit, or nil."
  [test op copied]
  (case (:f op)
    :restore-journal
    (when-let [node @copied]
      (reset! copied nil)
      (when (get (c/on-nodes test [node] ec-db/restore-journal!) node)
        node))
    (let [node  (rand-nth (vec (:nodes test)))
          fault (case (:f op)
                  :lose-journal     ec-db/lose-journal!
                  :lose-disk        ec-db/lose-disk!
                  :snapshot-journal ec-db/snapshot-journal!)]
      (c/on-nodes test [node] fault)
      (when (= :snapshot-journal (:f op))
        (reset! copied node))
      node)))

(defn- journal-nemesis
  "Loses or rolls back one node's journal at a time (#340):
    * `:lose-journal` deletes its journal and keeps its store;
    * `:lose-disk` deletes both, and has it come back joining;
    * `:snapshot-journal` copies its journal aside, and `:restore-journal` puts
      that copy back later, rolling the journal back.
  Each kills the node first and starts it after. Returns the node it hit."
  []
  (let [copied (atom nil)
        ;; The node the last fault hit.
        last-hit (atom nil)]
    (reify
      n/Reflection
      (fs [_] #{:lose-journal :lose-disk :snapshot-journal :restore-journal})
      n/Nemesis
      (setup! [this _test] this)
      (invoke! [_ test op]
        ;; One journal at a time: while the node the last fault hit is not back
        ;; yet, the fault is skipped. Losing more journals at once than a shard
        ;; tolerates would leave no contact to vouch for any of them (#312).
        ;; Never waited for: the nemeses share one worker, and a wait here would
        ;; hold back the others' heals.
        (let [back (or (nil? @last-hit) (ready? @last-hit 500))]
          (if-not back
            (assoc op :value {:skipped @last-hit})
            (let [hit (journal-fault! test op copied)]
              (when hit (reset! last-hit hit))
              (assoc op :value (or hit :no-copy))))))
      (teardown! [_ _test]))))

(defn- journal-package
  "The journal faults (#340), one every 25 s, in turn: a copy taken, a journal
  lost, the copy put back, a disk lost."
  []
  {:nemesis   (journal-nemesis)
   :generator (gen/delay 25
                         (cycle [{:type :info, :f :snapshot-journal}
                                 {:type :info, :f :lose-journal}
                                 {:type :info, :f :restore-journal}
                                 {:type :info, :f :lose-disk}]))
   :perf      #{{:name  "journal"
                 :fs    #{:lose-journal :lose-disk :snapshot-journal :restore-journal}
                 :color "#C9A0E5"}}})

(defn- parse-faults
  "Parse a comma-separated fault list into a set of keywords."
  [s]
  (set (map keyword (remove str/blank? (str/split s #",")))))

(def cli-opts
  "Extra CLI options on top of Jepsen's defaults."
  [[nil "--consistency-model MODEL"
    "Elle consistency model to check (e.g. strict-serializable, serializable)."
    :default :strict-serializable
    :parse-fn keyword]
   [nil "--faults FAULTS"
    "Comma-separated nemesis faults: partition,kill,pause,journal,clock (clock needs real VMs)."
    :default #{:partition :kill :pause}
    :parse-fn parse-faults]
   [nil "--linearizable-reads"
    "Enable evento-accord read barriers (linearizable reads; needed for strict-serializable)."
    :default false]
   [nil "--partition-targets TARGETS"
    "Comma-separated partition shapes: one,majority,majorities-ring. `one` keeps a
    quorum live (so linearizable reads stay available); the default can strand
    everyone."
    :default [:one :majority :majorities-ring]
    :parse-fn (fn [s] (mapv keyword (remove str/blank? (str/split s #","))))]])

(defn evento-test
  "Builds the Jepsen test map from CLI opts."
  [opts]
  (let [;; Canonical Elle list-append checker — we only borrow its checker, not
        ;; its (mixed read/append) generator. evento-accord *claims* strict
        ;; serializability, so that is the default bar; --consistency-model lets
        ;; you probe weaker models (a local read off a lagging replica is
        ;; serializable but not linearizable, so :serializable holds where
        ;; :strict-serializable does not).
        model   (:consistency-model opts)
        faults  (:faults opts)
        db      (ec-db/db)
        ;; Give Elle a generous per-SCC cycle-search budget: under high contention
        ;; the dependency graph forms one huge strongly-connected component, and the
        ;; default budget times out *inconclusively* (reported as a cycle-search-
        ;; timeout, not a confirmed anomaly) before it can verify it.
        elle    (:checker (append/test {:key-count            key-count
                                        :max-txn-length       max-append-keys
                                        :cycle-search-timeout 60000
                                        :consistency-models   [model]}))
        ;; Build ONLY the requested packages and compose them. We can't use
        ;; nc/nemesis-package: it always constructs every package (incl.
        ;; file-corruption, whose setup! downloads a tool and crashes offline) —
        ;; :faults only gates each package's *generator*, not its setup. Kill
        ;; drives db/Kill (SIGKILL + restart → journal recovery); pause drives
        ;; db/Pause (SIGSTOP/SIGCONT); both already implemented in evento-accord.db.
        npkg    (-> {:db        db
                     :nodes     (:nodes opts)
                     :faults    faults
                     :partition {:targets (:partition-targets opts)}
                     :kill      {:targets [:one :all]}
                     :pause     {:targets [:one :all]}
                     :interval  10})
        nem     (nc/compose-packages
                  (cond-> [(ready-package)]
                    (faults :partition)                  (conj (nc/partition-package npkg))
                    (some faults [:kill :pause])          (conj (nc/db-package npkg))
                    (faults :journal)                     (conj (journal-package))
                    (faults :clock)                       (conj (nc/clock-package npkg))))]
    (merge tests/noop-test
           opts
           {:name      (str "evento-accord-"
                            (if (seq faults) (str/join "+" (sort (map name faults))) "healthy")
                            (when (:linearizable-reads opts) "-linreads"))
            :os        os/noop
            :db        db
            :client    (ec-client/client)
            :nemesis   (:nemesis nem)
            :checker   (checker/compose
                         {:perf  (checker/perf {:nemeses (:perf nem)})
                          :stats (checker/stats)
                          :elle  elle})
            :generator (gen/phases
                         (->> gen-op
                              (gen/stagger 1/50)
                              (gen/nemesis (:generator nem))
                              (gen/time-limit (:time-limit opts)))
                         ;; Resolve every outstanding fault, wait until every node
                         ;; serves again, then let it converge before the final
                         ;; reads.
                         (gen/nemesis (:final-generator nem))
                         (gen/nemesis (gen/once {:type :info, :f :await-ready}))
                         (gen/sleep 15))})))

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn evento-test, :opt-spec cli-opts})
            args))
