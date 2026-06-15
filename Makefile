up:
	docker compose up -d --remove-orphans

stop:
	docker compose stop

down:
	docker compose down -v --remove-orphans

reset: down up

check: test fmt lint machete

test: test.sql test.core test.fjall test.accord test.migrator test.doc

test.sql:
	cargo test --all-features -p evento-sql

test.core:
	cargo test --all-features -p evento-core

test.fjall:
	cargo test --all-features -p evento-fjall

# The Accord consensus crate (simulation, cluster, membership, electorate, TCP/mTLS,
# recovery, …). The fjall/sql journal tests live with their backends (test.fjall /
# test.sql, which run them via each crate's `accord` feature).
test.accord:
	cargo test -p evento-accord

# The SQL migrations, including the optional accord consensus-journal migration.
test.migrator:
	cargo test --all-features -p evento-sql-migrator

test.doc:
	cargo test --doc -p evento

# Independent, adversarial verification of evento-accord (Docker + Jepsen + Elle).
# Requires Docker; everything else runs in containers. See evento-accord/jepsen.
jepsen:
	cd evento-accord/jepsen && ./run.sh

fmt:
	cargo fmt -- --emit files

lint:
	cargo clippy --all-features -- -D warnings

lint.fix:
	cargo clippy --fix --all-features -- -D warnings

machete:
	cargo machete --with-metadata

advisory.clean:
	rm -rf ~/.cargo/advisory-db

pants: advisory.clean
	cargo pants

audit: advisory.clean
	cargo audit

outdated:
	cargo outdated

dev:
	cargo watch -x "run -p todos"

# bank-axum-accord: the Accord-backed bank demo. Run standalone, or as a 3-node
# localhost TCP cluster (NODE_ID=0..2 → Accord port 7000+id, web port 3000+id).
accord:
	cargo run -p bank-axum-accord

accord.node0:
	NODE_ID=0 cargo run -p bank-axum-accord

accord.node1:
	NODE_ID=1 cargo run -p bank-axum-accord

accord.node2:
	NODE_ID=2 cargo run -p bank-axum-accord

# Run the whole 3-node cluster at once (each node in its own process).
accord.cluster:
	$(MAKE) accord.node0 accord.node1 accord.node2 -j3

# dev:
# 	$(MAKE) _dev -j2
#
# _dev: serve.shop serve.market
#
# serve.shop:
# 	cargo watch -x 'run -p shop'
#
# serve.market:
# 	cargo watch -x 'run -p market'

