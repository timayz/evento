up:
	docker compose up -d --remove-orphans

stop:
	docker compose stop

down:
	docker compose down -v --remove-orphans

reset: down up

check: test fmt lint machete

test:
	cargo test --all-features -p evento

fmt:
	cargo fmt -- --emit files

lint:
	cargo clippy --all-features -- -D warnings

lint.fix:
	cargo clippy --fix --all-features -- -D warnings

machete:
	cargo machete

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

# ACCORD Simulation
# Run continuous simulation fuzzer (Ctrl+C to stop)
sim:
	cargo run --example sim_runner -p evento-accord --release

# Debug a specific seed: make sim.debug SEED=12345
sim.debug:
	@if [ -z "$(SEED)" ]; then \
		echo "Usage: make sim.debug SEED=<seed>"; \
		echo "Example: make sim.debug SEED=10852234385963990305"; \
		exit 1; \
	fi
	cargo run --example sim_debug -p evento-accord --release -- $(SEED)

# Run ACCORD tests
sim.test:
	cargo test -p evento-accord

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

