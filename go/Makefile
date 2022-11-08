BINARY=clickhouse-client-program-go

clean:
	rm -rf ${BINARY}

format: 
	go fmt main.go

build: clean format
	go build -o ${BINARY}

run: clean format build
	./${BINARY}
