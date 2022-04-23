# NOTES
# MacOS build: ensure libs from ORACLE_HOME/lib are available in /usr/local/lib since variable DYLD_LIBRARY_PATH is
# silently ignored on macOS in some cases.

TEST_MODULES = actions cmd components config constants file helper plugin-loader rdbms table-definition transform
HP_VERSION:=$(shell git describe --tags --abbrev=0 | tr -d '[:space:]')
ORA_VERSION=19.14
BUILD_DATE=$(shell date +%F)  # use +%FT%H:%M%z for format '2020-10-08T17:55+0100'
OSARCH=$(shell uname | tr "[:upper:]" "[:lower:]")
GOARCH=amd64
LD_FLAGS:=-X github.com/relloyd/halfpipe/cmd.version=${HP_VERSION} -X github.com/relloyd/halfpipe/cmd.buildDate=${BUILD_DATE} -X github.com/relloyd/halfpipe/cmd.osArch=${OSARCH}
LD_FLAGS_VERBOSE:="-v"
LD_FLAGS_DEV:=-ldflags "${LD_FLAGS}"
LD_FLAGS_RELEASE:=-ldflags "${LD_FLAGS} -s -w"
# GC_FLAGS_GOLAND is used to work around build errors produced because hp plugins appear to be compiled with different library versions due to path issues.
GC_FLAGS_GOLAND:=-gcflags "all=-N -l"

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

###############################################################################
# BUILD BINARIES
###############################################################################

.PHONY: build
build:
	PKG_CONFIG_PATH=$(CURDIR) CGO_ENABLED=1 go build -trimpath $(LD_FLAGS_DEV) $(GC_FLAGS_GOLAND) -o dist/hp main.go

.PHONY: build-so
build-so:
	PKG_CONFIG_PATH=$(CURDIR) CGO_ENABLED=1 go build -trimpath $(LD_FLAGS_DEV) $(GC_FLAGS_GOLAND) -buildmode=plugin -o dist/hp-oracle-plugin.so rdbms/oracle/main.go
	PKG_CONFIG_PATH=$(CURDIR) CGO_ENABLED=1 go build -trimpath $(LD_FLAGS_DEV) $(GC_FLAGS_GOLAND) -buildmode=plugin -o dist/hp-odbc-plugin.so rdbms/odbc/main.go

.PHONY: build-race
build-race:
	PKG_CONFIG_PATH=$(CURDIR) CGO_ENABLED=1 go build -trimpath $(LD_FLAGS_DEV) -race -o dist/hp main.go
	PKG_CONFIG_PATH=$(CURDIR) CGO_ENABLED=1 go build -trimpath $(LD_FLAGS_DEV) -buildmode=plugin -race -o dist/hp-oracle-plugin.so rdbms/oracle/main.go
	PKG_CONFIG_PATH=$(CURDIR) CGO_ENABLED=1 go build -trimpath $(LD_FLAGS_DEV) -buildmode=plugin -race -o dist/hp-odbc-plugin.so rdbms/odbc/main.go

# build-linux target should be used inside Docker else the linking to OCI shared libraries are bad at runtime.
.PHONY: build-linux
build-linux: check-ora-vars
	# Pre-requisites:
	# Ensure Oracle instant client libraries and header files can be found during compilation:
	# LIBRARY_PATH and C_INCLUDE_PATH can be set to achieve this.
	CGO_ENABLED=1 GOOS=$(OSARCH) GOARCH=$(GOARCH) go build -v -trimpath $(LD_FLAGS_RELEASE) -o dist/hp main.go
	CGO_ENABLED=1 GOOS=$(OSARCH) GOARCH=$(GOARCH) go build -v -trimpath $(LD_FLAGS_RELEASE) -buildmode=plugin -o dist/hp-oracle-plugin.so rdbms/oracle/main.go
	CGO_ENABLED=1 GOOS=$(OSARCH) GOARCH=$(GOARCH) go build -v -trimpath $(LD_FLAGS_RELEASE) -buildmode=plugin -o dist/hp-odbc-plugin.so rdbms/odbc/main.go

###############################################################################
# BUILD & INSTALL NATIVE BINARIES
###############################################################################

.PHONY: install
install: build
	cp -p dist/hp $$GOPATH/bin/

.PHONY: install-so
install-so: build build-so
	cp -p dist/hp-oracle-plugin.so /usr/local/lib
	cp -p dist/hp-odbc-plugin.so /usr/local/lib

###############################################################################
# LINUX BUILDS VIA DOCKER
###############################################################################

.PHONY: docker-build
docker-build:
	scripts/docker-build.sh $(ORA_VERSION) $(HP_VERSION) relloyd/halfpipe-oracle-$(ORA_VERSION)

.PHONY: docker-run
docker-run:
	mkdir -p $$HOME/.halfpipe && \
	docker run -ti --rm \
		-v $$HOME/.halfpipe:/home/dataops/.halfpipe \
		relloyd/halfpipe-oracle-$(ORA_VERSION):latest

###############################################################################
# QUICKSTART
#
# 1. Build the core halfpipe docker image with additional tools like
#    AWS CLI, kubectl, k9s, less and other CLI hacks.
# 2. Start the image with .aws and .halfpipe directories mounted
#    and AWS_PROFILE=halfpipe
#
###############################################################################

.PHONY: quickstart
quickstart: docker-build
	scripts/start-halfpipe.sh relloyd/halfpipe-oracle-$(ORA_VERSION):latest

###############################################################################
# RELEASES
###############################################################################

.PHONY: docker-get-files
docker-get-files:
	$(eval RELEASE_DIR=dist/hp-linux-amd64-$(HP_VERSION)-oracle-$(ORA_VERSION))
	mkdir -p $(RELEASE_DIR)
	$(eval id=$(shell docker create relloyd/halfpipe-oracle-$(ORA_VERSION)):$(HP_VERSION)))
	docker cp $(id):/usr/local/bin/hp $(RELEASE_DIR)
	docker cp $(id):/usr/local/lib/hp-odbc-plugin.so $(RELEASE_DIR)
	docker cp $(id):/usr/local/lib/hp-oracle-plugin.so $(RELEASE_DIR)
	docker rm -v $(id)

.PHONY: release
release: release-darwin release-linux
	@echo Release complete

.PHONY: release-darwin
release-darwin:
	$(eval RELEASE_DIR=dist/hp-$(OSARCH)-$(GOARCH)-$(HP_VERSION)-oracle-$(ORA_VERSION))
	PKG_CONFIG_PATH=$(CURDIR) CGO_ENABLED=1 go build -trimpath $(LD_FLAGS_RELEASE) -o $(RELEASE_DIR)/hp main.go
	PKG_CONFIG_PATH=$(CURDIR) CGO_ENABLED=1 go build -trimpath $(LD_FLAGS_RELEASE) -buildmode=plugin -o $(RELEASE_DIR)/hp-oracle-plugin.so rdbms/oracle/main.go
	PKG_CONFIG_PATH=$(CURDIR) CGO_ENABLED=1 go build -trimpath $(LD_FLAGS_RELEASE) -buildmode=plugin -o $(RELEASE_DIR)/hp-odbc-plugin.so rdbms/odbc/main.go
	@echo Release darwin complete

.PHONY: release-linux
release-linux: docker-build docker-get-files
	@echo Release linux complete
