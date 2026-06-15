#!/usr/bin/env bash
# Build the cluster images, bring up control + n1..n5, and run the Jepsen test.
#
# Usage:
#   ./run.sh                       # default: partition nemesis, 120s
#   TIME_LIMIT=300 ./run.sh        # longer run
#   ./run.sh --concurrency 20      # extra args are passed through to `lein run test`
#
# Results land in evento-accord/jepsen/store/ (mounted from the control container).
set -euo pipefail
cd "$(dirname "$0")"

COMPOSE="docker compose -f docker/docker-compose.yml"
SECRET="docker/secret"

# Footgun guard: jepsen's clock nemesis bumps CLOCK_REALTIME, which these
# containers share with the host kernel — running it here would skew YOUR machine's
# clock. Clock skew must run on real VMs. Force past this only if you know the host
# is disposable (ALLOW_CLOCK_SKEW=1).
if [[ " $* " == *clock* && "${ALLOW_CLOCK_SKEW:-0}" != "1" ]]; then
  echo "ERROR: --faults clock skews the shared host clock under Docker. Run it on" >&2
  echo "real VMs, or set ALLOW_CLOCK_SKEW=1 if this host's clock is disposable." >&2
  exit 1
fi

# 1. One-time SSH keypair shared between control (private) and nodes (public).
mkdir -p "$SECRET"
if [ ! -f "$SECRET/jepsen" ]; then
  echo "==> generating SSH keypair in $SECRET"
  ssh-keygen -t rsa -b 2048 -N '' -C jepsen -f "$SECRET/jepsen"
fi

# 2. Build images (compiles the Rust binary in a builder stage) and start cluster.
echo "==> building images"
$COMPOSE build
echo "==> starting cluster"
$COMPOSE up -d

# 3. Wait for node sshd to accept connections (bounded, so a dead node fails fast).
echo "==> waiting for nodes to accept SSH"
for n in n1 n2 n3 n4 n5; do
  tries=0
  until $COMPOSE exec -T control bash -c "ssh -o ConnectTimeout=2 root@$n true" 2>/dev/null; do
    tries=$((tries + 1))
    if [ "$tries" -ge 30 ]; then
      echo "ERROR: $n not reachable over SSH after 30 tries. Node logs:" >&2
      $COMPOSE logs "$n" | tail -20 >&2
      exit 1
    fi
    sleep 2
  done
  echo "    $n up"
done

# 4. Run the test from the control node.
echo "==> running test"
$COMPOSE exec -T control \
  lein run test \
    --nodes n1,n2,n3,n4,n5 \
    --username root \
    --ssh-private-key /root/.ssh/id_rsa \
    --concurrency 10 \
    --time-limit "${TIME_LIMIT:-120}" \
    "$@"

echo "==> done. Inspect evento-accord/jepsen/store/latest/ (results.edn, *.html)"
