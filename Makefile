.PHONY: build test test-integration lint mockgen

VERSION = $(shell git describe --tags --dirty --always)
MONGODB_STARTUP_TIMEOUT ?= 4

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-mongo.version=${VERSION}'" -o conduit-connector-mongo cmd/connector/main.go

test:
	go test $(GOTEST_FLAGS) ./...

test-integration:
	docker run --rm -d -p 27017:27017 --name mongodb mongo --replSet=test
	sleep $(MONGODB_STARTUP_TIMEOUT)
	docker exec mongodb mongosh --eval "rs.initiate();"
	export CONNECTION_URI=mongodb://localhost:27017/?directConnection=true && \
	go test $(GOTEST_FLAGS) ./...; ret=$$?; \
		docker stop mongodb; \
		exit $$ret

lint:
	golangci-lint run --config .golangci.yml

mockgen:
	mockgen -package mock -source source/source.go -destination source/mock/source.go
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go
