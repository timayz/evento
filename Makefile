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

prepare:
	cargo sqlx prepare --merged