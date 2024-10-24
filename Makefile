VERSION = 				$(shell git describe --tags --dirty --always)
MONGODB_STARTUP_TIMEOUT ?= 4

.PHONY: build
build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-mongo.version=${VERSION}'" -o conduit-connector-mongo cmd/connector/main.go

.PHONY: test
test:
	go test $(GOTEST_FLAGS) ./...

.PHONY: test-integration
test-integration:
	docker run --rm -d -p 27017:27017 --name mongodb mongo --replSet=test
	sleep $(MONGODB_STARTUP_TIMEOUT)
	docker exec mongodb mongosh --eval "rs.initiate();"
	export CONNECTION_URI=mongodb://localhost:27017/?directConnection=true && \
	go test $(GOTEST_FLAGS) ./...; ret=$$?; \
		docker stop mongodb; \
		exit $$ret

.PHONY: lint
lint:
	golangci-lint run

.PHONY: generate
generate:
	go generate ./...

.PHONY: install-tools
install-tools:
	@echo Installing tools from tools.go
	@go list -e -f '{{ join .Imports "\n" }}' tools.go | xargs -I % go list -f "%@{{.Module.Version}}" % | xargs -tI % go install %
	@go mod tidy
