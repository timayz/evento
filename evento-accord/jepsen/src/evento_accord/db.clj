(ns evento-accord.db
  "Cluster lifecycle: starts/stops the prebuilt `jepsen-node` binary on each node.

  The binary is baked into the node image at `/opt/jepsen-node/jepsen-node`. We
  generate a tiny launcher that exports the per-node config (NODE_ID/PEERS/…) and
  execs the binary, then run it under start-stop-daemon. `exec` replaces the shell
  in place, so the daemon's pid (and process name `jepsen-node`) is the binary's —
  kill/pause/resume target it directly."
  (:require [clojure.string :as str]
            [clojure.tools.logging :refer [info]]
            [jepsen.control :as c]
            [jepsen.control.util :as cu]
            [jepsen.db :as db]))

(def dir          "/opt/jepsen-node")
(def binary       (str dir "/jepsen-node"))
(def launcher     (str dir "/run.sh"))
(def logfile      (str dir "/node.log"))
(def pidfile      (str dir "/node.pid"))
(def data-dir     "/var/lib/jepsen-node")
(def accord-port  7000)
(def http-port    8080)

(defn node-id
  "Stable integer id for a node = its index in the sorted node list."
  [test node]
  (.indexOf ^java.util.List (vec (sort (:nodes test))) node))

(defn peers-str
  "PEERS env: `id=host:7000` for every node, comma-separated."
  [test]
  (->> (sort (:nodes test))
       (map-indexed (fn [i n] (str i "=" n ":" accord-port)))
       (str/join ",")))

(defn- install-launcher!
  "Writes the per-node launch script (env + exec) onto the node."
  [test node]
  (c/exec :mkdir :-p dir data-dir)
  (let [script (str "#!/bin/bash\n"
                    "export NODE_ID=" (node-id test node) "\n"
                    "export PEERS='" (peers-str test) "'\n"
                    "export LISTEN=0.0.0.0:" accord-port "\n"
                    "export HTTP_PORT=" http-port "\n"
                    "export DATA_DIR=" data-dir "\n"
                    "export LINEARIZABLE_READS=" (if (:linearizable-reads test) "1" "0") "\n"
                    "exec " binary "\n")]
    (c/exec :bash :-c
            (str "cat > " launcher " <<'LAUNCH_EOF'\n" script "LAUNCH_EOF"))
    (c/exec :chmod :+x launcher)))

(defn- pkill!
  "Send `signal` to the jepsen-node process(es). Matches the exact process *name*
  (`-x`), NOT the command line — matching the path with `-f` would also match the
  shell running pkill and make it kill itself. `|| true` so a no-match isn't an
  error (pkill exits 1 when nothing matched)."
  [signal]
  (c/exec :bash :-c (str "pkill -" (name signal) " -x jepsen-node || true")))

(defn- kill-all!
  "Force-kill any jepsen-node left over from a prior run. Without this, a survivor
  keeps the TCP ports + an open Fjall store, so a freshly-started binary can't bind
  and the workload silently hits stale data."
  []
  (pkill! :KILL))

(defn- start-node!
  [test node]
  (install-launcher! test node)
  (cu/start-daemon!
    {:logfile logfile
     :pidfile pidfile
     :chdir   dir}
    launcher))

(defn db
  "evento-accord DB built from the prebuilt binary baked into the node image."
  []
  (reify db/DB
    (setup! [_ test node]
      (info node "starting jepsen-node id" (node-id test node))
      ;; Kill survivors and wipe state before starting, so each run is clean.
      (kill-all!)
      (c/exec :rm :-rf data-dir)
      (start-node! test node)
      (cu/await-tcp-port http-port))

    (teardown! [_ _test node]
      (info node "tearing down jepsen-node")
      (cu/stop-daemon! binary pidfile)
      (kill-all!)
      (c/exec :rm :-rf data-dir logfile pidfile))

    db/LogFiles
    (log-files [_ _test _node]
      [logfile])

    ;; So the nemesis can kill/restart real OS processes (layered in after the
    ;; partition baseline is green).
    db/Kill
    (start! [_ test node]
      (start-node! test node))
    (kill! [_ _test _node]
      (pkill! :KILL))

    db/Pause
    (pause! [_ _test _node]
      (pkill! :STOP))
    (resume! [_ _test _node]
      (pkill! :CONT))))
