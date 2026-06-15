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
  and process pause (SIGSTOP/SIGCONT) with a unified schedule. Clock skew (:clock) is
  *available* but excluded by default — it bumps the kernel's wall clock, which a
  shared-kernel Docker cluster cannot isolate from the host, so it belongs on real
  VMs (and needs a C compiler on the node image for jepsen's time helper)."
  (:require [clojure.string :as str]
            [jepsen [cli :as cli]
                    [checker :as checker]
                    [generator :as gen]
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
    "Comma-separated nemesis faults: partition,kill,pause,clock (clock needs real VMs)."
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
                  (cond-> []
                    (faults :partition)                  (conj (nc/partition-package npkg))
                    (some faults [:kill :pause])          (conj (nc/db-package npkg))
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
                         ;; Resolve every outstanding fault, then let it converge
                         ;; before the final reads.
                         (gen/nemesis (:final-generator nem))
                         (gen/sleep 15))})))

(defn -main
  [& args]
  (cli/run! (cli/single-test-cmd {:test-fn evento-test, :opt-spec cli-opts})
            args))
