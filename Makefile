create:
	sqlx database create --database-url postgres://postgres:postgres@localhost:5432/evento_example

migrate:
	sqlx migrate run --database-url postgres://postgres:postgres@localhost:5432/evento_example
