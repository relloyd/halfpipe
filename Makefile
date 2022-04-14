# NOTES
# MacOS build: ensure libs from ORACLE_HOME/lib are available in /usr/local/lib since variable DYLD_LIBRARY_PATH is
# silently ignored on macOS in some cases.

TEST_MODULES = actions cmd components config constants file helper plugin-loader rdbms table-definition transform
HP_VERSION:=$(shell git describe --tags --abbrev=0 | tr -d '[:space:]')
ORA_VERSION=19.8
OSARCH=$(shell uname | tr "[:upper:]" "[:lower:]")
GOARCH=amd64
BUILD_DATE=$(shell date +%F)  # use +%FT%H:%M%z for format '2020-10-08T17:55+0100'
LD_FLAGS:=-X github.com/relloyd/halfpipe/cmd.version=${HP_VERSION} -X github.com/relloyd/halfpipe/cmd.buildDate=${BUILD_DATE} -X github.com/relloyd/halfpipe/cmd.osArch=${OSARCH}
LD_FLAGS_VERBOSE:="-v"
LD_FLAGS_DEV:=-ldflags "${LD_FLAGS}"
LD_FLAGS_RELEASE:=-ldflags "${LD_FLAGS} -s -w"
# GC_FLAGS_GOLAND is used to work around build errors produced because hp plugins appear to be compiled with different library versions due to path issues.
GC_FLAGS_GOLAND:=-gcflags "all=-N -l"
GOPRIVATE=GOPRIVATE=github.com/relloyd

define command-export-aws-keys
	$(eval export AWS_ACCESS_KEY_ID=$(shell aws configure get aws_access_key_id --profile halfpipe))
	$(eval export AWS_SECRET_ACCESS_KEY=$(shell aws configure get aws_secret_access_key --profile halfpipe))
endef

.PHONY: default
default: install

set-aws-keys:
	$(call command-export-aws-keys)

.PHONY: check-test-vars
check-test-vars:
ifndef AWS_ACCESS_KEY_ID
	$(error AWS_ACCESS_KEY_ID is not set)
endif
ifndef AWS_SECRET_ACCESS_KEY
	$(error AWS_SECRET_ACCESS_KEY is not set)
endif

.PHONY: check-ora-vars
check-ora-vars:
ifndef ORA_VERSION
	$(error ORA_VERSION is not set)
endif

.PHONY: check-directory-var
check-directory-var:
ifndef DIR
	$(error DIR is not set)
endif

.PHONY: tag-prerelease
tag-prerelease:
	$(eval NEXT_TAG=$(shell git describe --tags --abbrev=0 | awk -F. '{printf "%s.%s.%s-pre", $$1, $$2, $$3+1}'))
	git tag -a "$(NEXT_TAG)" -m "$(NEXT_TAG)"
	echo "Bumped tag to: $(NEXT_TAG)"

.PHONY: tag-next
tag-next:
	$(eval NEXT_TAG=$(shell git describe --tags --abbrev=0 | awk -F. '{printf "%s.%s.%s", $$1, $$2, $$3+1}'))
	@git tag -a "$(NEXT_TAG)" -m "$(NEXT_TAG)"
	@echo "Bumped tag to: $(NEXT_TAG)"

###############################################################################
# WORKERS
###############################################################################

.PHONY: docker-worker-linux
docker-worker-linux:
	cd image && docker build -t worker-linux --build-arg ORA_VERSION=$(ORA_VERSION) -f Dockerfile-worker-linux .

.PHONY: docker-worker-alpine
docker-worker-alpine:
	cd image && docker build -t worker-alpine --build-arg ORA_VERSION=$(ORA_VERSION) -f Dockerfile-worker-alpine .

.PHONY: docker-compile-hp-linux
docker-compile-hp-linux:
	scripts/docker-build.sh $(ORA_VERSION) hp-linux-$(HP_VERSION) relloyd/halfpipe-built-oracle-$(ORA_VERSION) hp-compiled

.PHONY: docker-build-alpine
docker-build-alpine:
	scripts/docker-build.sh $(ORA_VERSION) hp-linux-$(HP_VERSION) relloyd/halfpipe-alpine-oracle-$(ORA_VERSION) alpine

###############################################################################
# TESTING
###############################################################################

.PHONY: test-dir
test-dir: check-directory-var
	export AWS_PROFILE=halfpipe && \
	cd $(DIR) && \
	go test -trimpath $(LD_FLAGS_DEV) $(GC_FLAGS_GOLAND)

.PHONY: test-integration
test-integration: check-directory-var
	export AWS_PROFILE=halfpipe && \
	cd cmd && \
	go test -trimpath $(LD_FLAGS_DEV) $(GC_FLAGS_GOLAND) -tags=integration

.PHONY: test-modules
test-modules:
	export AWS_PROFILE=halfpipe && \
	for dir in $(TEST_MODULES); do \
		 go test -trimpath $(LD_FLAGS_DEV) $(GC_FLAGS_GOLAND) ./$$dir/... || exit 1 ; \
	done

.PHONY: test
test: test-modules

.PHONY: install
install: build
	cp -p dist/hp ~/go/bin/

###############################################################################
# BUILD BINARIES
###############################################################################

.PHONY: build
build:
	PKG_CONFIG_PATH=$(CURDIR) CGO_ENABLED=1 $(GOPRIVATE) go build -trimpath $(LD_FLAGS_DEV) $(GC_FLAGS_GOLAND) -o dist/hp main.go

.PHONY: build-so
build-so:
	CGO_ENABLED=1 $(GOPRIVATE) go build -trimpath $(LD_FLAGS_DEV) $(GC_FLAGS_GOLAND) -buildmode=plugin -o dist/hp-oracle-plugin.so rdbms/oracle/main.go
	CGO_ENABLED=1 $(GOPRIVATE) go build -trimpath $(LD_FLAGS_DEV) $(GC_FLAGS_GOLAND) -buildmode=plugin -o dist/hp-odbc-plugin.so rdbms/odbc/main.go
	cp -p dist/hp-oracle-plugin.so /usr/local/lib
	cp -p dist/hp-odbc-plugin.so /usr/local/lib

.PHONY: build-race
build-race:
	PKG_CONFIG_PATH=$(CURDIR) CGO_ENABLED=1 $(GOPRIVATE) go build -trimpath $(LD_FLAGS_DEV) -race -o dist/hp main.go
	PKG_CONFIG_PATH=$(CURDIR) CGO_ENABLED=1 $(GOPRIVATE) go build -trimpath $(LD_FLAGS_DEV) -buildmode=plugin -race -o dist/hp-oracle-plugin.so rdbms/oracle/main.go
	PKG_CONFIG_PATH=$(CURDIR) CGO_ENABLED=1 $(GOPRIVATE) go build -trimpath $(LD_FLAGS_DEV) -buildmode=plugin -race -o dist/hp-odbc-plugin.so rdbms/odbc/main.go

# build-linux target should be used inside Docker else the linking to OCI shared libraries are bad at runtime.
.PHONY: build-linux
build-linux: check-ora-vars
	CGO_ENABLED=1 $(GOPRIVATE) GOOS=$(OSARCH) GOARCH=$(GOARCH) go build -trimpath $(LD_FLAGS_RELEASE) -o dist/$(BINARY_NAME) main.go
	CGO_ENABLED=1 $(GOPRIVATE) GOOS=$(OSARCH) GOARCH=$(GOARCH) go build -trimpath $(LD_FLAGS_RELEASE) -buildmode=plugin -o dist/hp-oracle-plugin.so rdbms/oracle/main.go
	CGO_ENABLED=1 $(GOPRIVATE) GOOS=$(OSARCH) GOARCH=$(GOARCH) go build -trimpath $(LD_FLAGS_RELEASE) -buildmode=plugin -o dist/hp-odbc-plugin.so rdbms/odbc/main.go

.PHONY: build-alpine
build-alpine: check-ora-vars
	CGO_ENABLED=1 $(GOPRIVATE) go build -trimpath $(LD_FLAGS_RELEASE) -o dist/$(BINARY_NAME) main.go
	CGO_ENABLED=1 $(GOPRIVATE) go build -trimpath $(LD_FLAGS_RELEASE) -buildmode=plugin -o dist/hp-oracle-plugin.so rdbms/oracle/main.go
	CGO_ENABLED=1 $(GOPRIVATE) go build -trimpath $(LD_FLAGS_RELEASE) -buildmode=plugin -o dist/hp-odbc-plugin.so rdbms/odbc/main.go

###############################################################################
# LINUX BUILDS VIA DOCKER
###############################################################################

.PHONY: docker-build
docker-build:
	scripts/docker-build.sh $(ORA_VERSION) hp-linux-amd64-$(HP_VERSION) relloyd/halfpipe-oracle-$(ORA_VERSION)-no-oci

.PHONY: docker-push-latest
docker-push-latest:
	scripts/docker-push-latest.sh $(ORA_VERSION)

.PHONY: docker-get-files
docker-get-files:
	$(eval RELEASE_DIR=dist/hp-linux-amd64-$(HP_VERSION)-oracle-$(ORA_VERSION))
	mkdir -p $(RELEASE_DIR)
	$(eval id=$(shell docker create relloyd/halfpipe-oracle-19.8-no-oci:$(HP_VERSION)))
	docker cp $(id):/usr/local/bin/hp-linux-amd64-$(HP_VERSION)-oracle-$(ORA_VERSION) $(RELEASE_DIR)/hp
	docker cp $(id):/usr/local/lib/hp-odbc-plugin.so $(RELEASE_DIR)
	docker cp $(id):/usr/local/lib/hp-oracle-plugin.so $(RELEASE_DIR)
	docker rm -v $(id)

###############################################################################
# RELEASES
###############################################################################

.PHONY: release
release: release-darwin release-linux
	@echo Release complete

.PHONY: release-darwin
release-darwin:
# TODO: make the plugins have the same version in their name OR remove version from name.
	$(eval RELEASE_DIR=dist/hp-$(OSARCH)-$(GOARCH)-$(HP_VERSION)-oracle-$(ORA_VERSION))
	PKG_CONFIG_PATH=$(CURDIR) CGO_ENABLED=1 $(GOPRIVATE) go build -trimpath $(LD_FLAGS_RELEASE) -o $(RELEASE_DIR)/hp main.go
	PKG_CONFIG_PATH=$(CURDIR) CGO_ENABLED=1 $(GOPRIVATE) go build -trimpath $(LD_FLAGS_RELEASE) -buildmode=plugin -o $(RELEASE_DIR)/hp-oracle-plugin.so rdbms/oracle/main.go
	PKG_CONFIG_PATH=$(CURDIR) CGO_ENABLED=1 $(GOPRIVATE) go build -trimpath $(LD_FLAGS_RELEASE) -buildmode=plugin -o $(RELEASE_DIR)/hp-odbc-plugin.so rdbms/odbc/main.go
	@echo Release darwin complete

.PHONY: release-linux
release-linux: docker-build docker-push-latest docker-get-files
	@echo Release linux complete
