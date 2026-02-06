# Northbound plugin Makefile

.PHONY: all build clean xunji http mqtt

PLUGIN_DIR := ..
OUTPUT_DIR := $(PLUGIN_DIR)

all: build

build: xunji http mqtt
	@echo "Northbound plugins built in $(OUTPUT_DIR)"

xunji: src/northbound-xunji/main.go
	@mkdir -p $(OUTPUT_DIR)
	CGO_ENABLED=0 go build -ldflags "-s -w" -o $(OUTPUT_DIR)/northbound-xunji ./src/northbound-xunji

http: src/northbound-http/main.go
	@mkdir -p $(OUTPUT_DIR)
	CGO_ENABLED=0 go build -ldflags "-s -w" -o $(OUTPUT_DIR)/northbound-http ./src/northbound-http

mqtt: src/northbound-mqtt/main.go
	@mkdir -p $(OUTPUT_DIR)
	CGO_ENABLED=0 go build -ldflags "-s -w" -o $(OUTPUT_DIR)/northbound-mqtt ./src/northbound-mqtt

clean:
	rm -f $(OUTPUT_DIR)/northbound-xunji $(OUTPUT_DIR)/northbound-http $(OUTPUT_DIR)/northbound-mqtt
