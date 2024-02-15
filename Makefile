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

dev:
	$(MAKE) _dev -j6

_dev: serve.shop serve.inventory serve.payment tailwind.shop tailwind.inventory tailwind.payment

serve.shop:
	cargo watch -x 'run -p shop'

serve.inventory:
	cargo watch -x 'run -p inventory'

serve.payment:
	cargo watch -x 'run -p payment'

tailwind.shop:
	npx tailwindcss -i ./examples/shop/style/tailwind.css -o ./examples/shop/public/main.css --watch

tailwind.inventory:
	npx tailwindcss -i ./examples/inventory/style/tailwind.css -o ./examples/inventory/public/main.css --watch

tailwind.payment:
	npx tailwindcss -i ./examples/payment/style/tailwind.css -o ./examples/payment/public/main.css --watch
