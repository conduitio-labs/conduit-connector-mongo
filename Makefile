VERSION = 				$(shell git describe --tags --dirty --always)

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-mongo.version=${VERSION}'" -o conduit-connector-mongo cmd/connector/main.go

.PHONY: test
test:
	go test $(GOTEST_FLAGS) ./...

.PHONY: test-integration
test-integration:
	docker compose -f test/compose.yaml up --quiet-pull -d --wait
	export CONNECTION_URI=mongodb://localhost:27017/?directConnection=true && \
	go test $(GOTEST_FLAGS) ./...; ret=$$?; \
		docker compose -f test/compose.yaml down; \
		exit $$ret

.PHONY: lint
lint:
	golangci-lint config verify
	golangci-lint run

.PHONY: generate
generate:
	go generate ./...
	conn-sdk-cli readmegen -w

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -I % go list -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy
