SHELL := /bin/bash

init:
	dep ensure -v
	go install ./vendor/...

up_docker_server:
	test -n "$$(docker ps -a --format {{.Names}} | grep -o dbr-clickhouse-server)" && docker rm -f dbr-clickhouse-server; \
	docker run -p 127.0.0.1:8123:8123 --name dbr-clickhouse-server -d yandex/clickhouse-server;

test:
	test -z "$$(golint ./... | grep -v vendor | tee /dev/stderr)"
	go vet ./...
	test -z "$$(gofmt -d -s $$(find . -name \*.go -print | grep -v vendor) | tee /dev/stderr)"
	go test -v -covermode=count -coverprofile=coverage.out .