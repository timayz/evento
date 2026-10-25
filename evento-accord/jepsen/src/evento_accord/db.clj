(ns evento-accord.db
  "Cluster lifecycle: starts/stops the prebuilt `jepsen-node` binary on each node.

  The binary is baked into the node image at `/opt/jepsen-node/jepsen-node`. We
  generate a tiny launcher that exports the per-node config (NODE_ID/PEERS/…) and
  runs the binary, then run it under start-stop-daemon. The launcher runs the
  binary again whenever it exits with 75 (`EX_TEMPFAIL`): that is how the node
  comes back joining after it lost or rolled back its journal (OPERATIONS §7).
  Any other exit — a SIGKILL included — ends the launcher, so a killed node stays
  down until the kill nemesis starts it. Kill/pause/resume target the binary by
  its process name, `jepsen-node`.

  The journal faults (#340) lose a node's journal, its whole disk, or roll its
  journal back to an older copy, each on a killed node that is then started."
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
(def journal-dir  (str data-dir "/journal"))
(def store-dir    (str data-dir "/store"))
(def join-marker  (str data-dir "/join"))
(def journal-copy (str dir "/journal-copy"))
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
                    ;; 75 (EX_TEMPFAIL): the node comes back joining.
                    "while true; do\n"
                    "  " binary "\n"
                    "  code=$?\n"
                    "  [ \"$code\" -eq 75 ] || exit \"$code\"\n"
                    "done\n")]
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
  "Starts the launcher, unless it runs already. It is found by its process name:
  a script's executable is its interpreter, so matching the executable would
  start a second one over a running node."
  [test node]
  (install-launcher! test node)
  (cu/start-daemon!
    {:logfile             logfile
     :pidfile             pidfile
     :chdir               dir
     :match-executable?   false
     :match-process-name? true
     :process-name        "run.sh"}
    launcher))

(defn- kill-node!
  "SIGKILLs the node and waits until it is gone, its launcher with it."
  []
  (pkill! :KILL)
  ;; `[r]un.sh`: a pattern this shell's own command line does not match.
  (c/exec :bash :-c "while pgrep -x jepsen-node >/dev/null || pgrep -f 'jepsen-node/[r]un.sh' >/dev/null; do sleep 0.1; done"))

(defn lose-journal!
  "The node's journal is lost, its store kept: it is killed, its journal
  deleted, and started. A plain open over a store that holds events is
  refused (`LostJournal`), so it comes back joining over an empty journal."
  [test node]
  (kill-node!)
  (c/exec :rm :-rf journal-dir)
  (start-node! test node))

(defn lose-disk!
  "The node's disk is lost: killed, its journal and store deleted, and started
  joining — what its host knows to do with a member that lost everything."
  [test node]
  (kill-node!)
  (c/exec :rm :-rf journal-dir store-dir)
  (c/exec :touch join-marker)
  (start-node! test node))

(defn snapshot-journal!
  "Copies the node's journal aside, as a backup: killed, copied, started."
  [test node]
  (kill-node!)
  (c/exec :rm :-rf journal-copy)
  (c/exec :cp :-a journal-dir journal-copy)
  (start-node! test node))

(defn restore-journal!
  "Rolls the node's journal back to the copy `snapshot-journal!` took, if any:
  killed, restored, started. Its peers find it rolled back (#339), and it comes
  back joining over an empty journal. Returns whether there was a copy."
  [test node]
  (if (cu/exists? journal-copy)
    (do (kill-node!)
        (c/exec :rm :-rf journal-dir)
        (c/exec :cp :-a journal-copy journal-dir)
        (c/exec :rm :-rf journal-copy)
        (start-node! test node)
        true)
    false))

(defn db
  "evento-accord DB built from the prebuilt binary baked into the node image."
  []
  (reify db/DB
    (setup! [_ test node]
      (info node "starting jepsen-node id" (node-id test node))
      ;; Kill survivors and wipe state before starting, so each run is clean.
      (kill-all!)
      (c/exec :rm :-rf data-dir journal-copy)
      (start-node! test node)
      (cu/await-tcp-port http-port))

    (teardown! [_ _test node]
      (info node "tearing down jepsen-node")
      (cu/stop-daemon! binary pidfile)
      (kill-all!)
      (c/exec :rm :-rf data-dir logfile pidfile journal-copy))

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
