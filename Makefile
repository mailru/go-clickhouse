SHELL := /bin/bash

init:
	GO111MODULE=on go mod vendor
	go install github.com/golangci/golangci-lint/...@v1.56.2

up_docker_server: stop_docker_server
	docker run --rm=true -p 8123:8123 -p 9000:9000 -e CLICKHOUSE_SKIP_USER_SETUP=1 --name dbr-clickhouse-server -d clickhouse/clickhouse-server:latest;
	sleep 5; # wait until clickhouse is initialized

stop_docker_server:
	test -n "$$(docker ps --format {{.Names}} | grep dbr-clickhouse-server)" && docker stop dbr-clickhouse-server || true

test: up_docker_server
	golangci-lint run -v ./...
	test -z "$$(gofmt -d -s $$(find . -name \*.go -print | grep -v vendor) | tee /dev/stderr)"
	go test -v -covermode=count -coverprofile=coverage.out . 
	$(MAKE) stop_docker_server
