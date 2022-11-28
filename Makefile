prepare:
	SQLX_OFFLINE=true cargo sqlx prepare --database-url postgres://postgres:postgres@localhost:5432/evento_example --merged

create:
	sqlx database create --database-url postgres://postgres:postgres@localhost:5432/evento_example

migrate:
	sqlx migrate run --database-url postgres://postgres:postgres@localhost:5432/evento_example
