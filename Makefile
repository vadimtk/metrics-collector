OS = darwin freebsd linux windows
ARCHS = 386 amd64

all: build release

build: deps
	go build

release: clean deps
	@for arch in $(ARCHS);\
	do \
		for os in $(OS);\
		do \
			echo "Building $$os-$$arch"; \
			mkdir -p build/metrics-collector-$$os-$$arch/; \
			GOOS=$$os GOARCH=$$arch go build -o build/metrics-collector-$$os-$$arch/metrics-collector; \
			tar cz -C build -f build/metrics-collector-$$os-$$arch.tar.gz metrics-collector-$$os-$$arch; \
		done \
	done

test: deps
	go test ./...

deps:
	go get -d -v -t ./...

clean:
	rm -rf build
	rm -f metrics-collector
