.PHONY: build test lint

VERSION = $(shell git describe --tags --dirty --always)
MONGODB_STARTUP_TIMEOUT ?= 4

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-mongo.version=${VERSION}'" -o conduit-connector-mongo cmd/connector/main.go

dock:
	docker run --rm -d -p 27017:27017 --name mongodb mongo --replSet=test
	docker exec -it mongodb mongosh --eval "rs.initiate();"

test:
	docker run --rm -d -p 27017:27017 --name mongodb mongo --replSet=test
	sleep $(MONGODB_STARTUP_TIMEOUT)
	docker exec mongodb mongosh --eval "rs.initiate();"
	go test $(GOTEST_FLAGS) ./...; ret=$$?; \
		docker stop mongodb; \
		exit $$ret

lint:
	golangci-lint run --config .golangci.yml

mockgen:
	mockgen -package mock -source source/source.go -destination source/mock/source.go
	mockgen -package mock -source destination/destination.go -destination destination/mock/destination.go
