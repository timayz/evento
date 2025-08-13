up:
	docker compose up -d --remove-orphans

stop:
	docker compose stop

down:
	docker compose down -v --remove-orphans

reset: down up

test:
	cargo test --features=full

fmt:
	cargo fmt -- --emit files

clippy:
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
	$(MAKE) _dev -j2

_dev: serve.shop serve.product 

serve.shop:
	cargo watch -x 'run -p shop'

serve.product:
	cargo watch -x 'run -p product'

