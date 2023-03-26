up:
	docker compose up -d --remove-orphans

stop:
	docker compose stop

down:
	docker compose down -v --remove-orphans

create:
	sqlx database create

migrate:
	sqlx migrate run

revert:
	sqlx migrate revert

prepare:
	cargo sqlx prepare --merged

fmt:
	cargo fmt -- --emit files

clippy:
	cargo clippy --fix --all-features -- -D warnings
	cargo clippy --all-features -- -D warnings
