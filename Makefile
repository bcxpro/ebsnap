BINARY_NAME=ebsnapshot
BUILD_VERSION?= $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

build:
	cd $(CURDIR)/src && GOARCH=amd64 GOOS=linux go build -o $(CURDIR)/src/out/${BINARY_NAME}-linux-amd64 -ldflags="-X 'ebsnapshot/version.versionString=${BUILD_VERSION}'" main.go 
	cd $(CURDIR)/src && GOARCH=arm64 GOOS=linux go build -o $(CURDIR)/src/out/${BINARY_NAME}-linux-arm64 -ldflags="-X 'ebsnapshot/version.versionString=${BUILD_VERSION}'" main.go 
	cd $(CURDIR)/src && GOARCH=amd64 GOOS=windows go build -o $(CURDIR)/src/out/${BINARY_NAME}-windows-amd64.exe -ldflags="-X 'ebsnapshot/version.versionString=${BUILD_VERSION}'" main.go 

test:
	cd $(CURDIR)/src && go test -v ./...