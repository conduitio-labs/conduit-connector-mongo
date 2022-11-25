.PHONY: build test lint

VERSION=$(shell git describe --tags --dirty --always)

build:
	go build -ldflags "-X 'github.com/conduitio-labs/conduit-connector-mongo.version=${VERSION}'" -o conduit-connector-mongo cmd/connector/main.go

test:
	docker run --rm -d -p 27017:27017 --name mongodb mongo --replSet=test && \
	sleep 4 && \
	docker exec -it mongodb mongosh --eval "rs.initiate();"
	go test $(GOTEST_FLAGS) ./... && \
	docker stop mongodb

lint:
	golangci-lint run --config .golangci.yml

mockgen:
	mockgen -package mock -source source/source.go -destination source/mock/source.go