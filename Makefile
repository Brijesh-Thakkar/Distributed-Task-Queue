ROOT_DIR:=$(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))

proto: internal/proto/dtq.proto
	protoc -I=$(ROOT_DIR)/internal/proto \
				 --go_out=$(ROOT_DIR)/internal/proto \
				 --go_opt=module=github.com/brijesh-thakkar/distributed-task-queue/internal/proto \
				 $(ROOT_DIR)/internal/proto/dtq.proto

.PHONY: lint
lint:
	golangci-lint run
