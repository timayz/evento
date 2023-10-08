up:
	docker compose up -d --remove-orphans

stop:
	docker compose stop

down:
	docker compose down -v --remove-orphans

reset: down up

create:
	sqlx database create

migrate:
	sqlx migrate run

revert:
	sqlx migrate revert

prepare:
	cargo sqlx prepare --merged

test:
	cargo test --features=full

fmt:
	cargo fmt -- --emit files

clippy:
	cargo clippy --fix --all-features -- -D warnings

deny:
	cargo deny check

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
