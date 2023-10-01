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
	cargo test

fmt:
	cargo fmt -- --emit files

clippy:
	cargo clippy --fix --all-features -- -D warnings

deny:
	cargo deny check

udeps:
	cargo udeps -p example -p evento -p evento-axum -p evento-query -p evento-store

advisory.clean:
	rm -rf ~/.cargo/advisory-db

pants: advisory.clean
	cargo pants

audit: advisory.clean
	cargo audit

outdated:
	cargo outdated
