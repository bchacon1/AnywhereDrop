# Local development and build entry points. See README.md.
GO ?= go
FRONTEND := frontend
BACKEND  := backend

.PHONY: dev dev-backend dev-frontend build run test lint e2e clean

## Run the Go backend (:8080) and the Vite dev server (:5173, proxies /health, /api, /ws) together.
dev:
	@trap 'kill 0' EXIT; \
	$(MAKE) dev-backend & \
	$(MAKE) dev-frontend & \
	wait

dev-backend:
	cd $(BACKEND) && $(GO) run ./cmd/server

dev-frontend:
	cd $(FRONTEND) && npm run dev

## Build the frontend, embed it into the Go binary at backend/bin/server.
build:
	cd $(FRONTEND) && npm run build
	rm -rf $(BACKEND)/web/dist && mkdir -p $(BACKEND)/web/dist && cp -R $(FRONTEND)/dist/. $(BACKEND)/web/dist/ && touch $(BACKEND)/web/dist/.gitkeep
	cd $(BACKEND) && $(GO) build -o bin/server ./cmd/server

## Run the single embedded binary on :8080.
run: build
	cd $(BACKEND) && PORT=8080 ./bin/server

test:
	cd $(BACKEND) && $(GO) vet ./... && $(GO) test -race ./...
	cd $(FRONTEND) && npm run typecheck && npm test

lint:
	cd $(FRONTEND) && npm run lint

## End-to-end tests against the embedded binary (runs `build` first).
e2e: build
	cd tests/e2e && npx playwright test --project=chromium

clean:
	rm -rf $(FRONTEND)/dist $(BACKEND)/bin
	find $(BACKEND)/web/dist -mindepth 1 ! -name .gitkeep -delete
