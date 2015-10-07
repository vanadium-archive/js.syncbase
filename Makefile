SHELL := /bin/bash -euo pipefail
export PATH := ./go/bin:$(JIRI_ROOT)/release/go/bin:node_modules/.bin:$(JIRI_ROOT)/third_party/cout/node/bin:$(PATH)

# This target causes any target files to be deleted if the target task fails.
.DELETE_ON_ERROR:

UNAME := $(shell uname)

# When running browser tests on non-Darwin machines, set the --headless flag.
# This uses Xvfb underneath the hood (inside prova => browser-launcher =>
# headless), which is not supported on OS X.
# See: https://github.com/kesla/node-headless/
ifndef NOHEADLESS
	ifneq ($(UNAME),Darwin)
		HEADLESS := --headless
	endif
endif

ifdef STOPONFAIL
	STOPONFAIL := --stopOnFirstFailure
endif

ifndef NOTAP
	TAP := --tap
endif

ifndef NOQUIT
	QUIT := --quit
endif

ifdef XUNIT
	TAP := --tap # TAP must be set for xunit to work
	OUTPUT_TRANSFORM := tap-xunit
endif

ifdef BROWSER_OUTPUT
	BROWSER_OUTPUT_LOCAL = $(BROWSER_OUTPUT)
	ifdef OUTPUT_TRANSFORM
		BROWSER_OUTPUT_LOCAL := >($(OUTPUT_TRANSFORM) --package=javascript.browser > $(BROWSER_OUTPUT_LOCAL))
	endif
	BROWSER_OUTPUT_LOCAL := | tee $(BROWSER_OUTPUT_LOCAL)
endif

PROVA_OPTS := --includeFilenameAsPackage $(TAP) $(QUIT) $(STOPONFAIL)

BROWSER_OPTS := --browser --launch chrome $(HEADLESS) --log=./tmp/chrome.log

.DEFAULT_GOAL := all

.PHONY: all
all:

go/bin: $(shell find $(JIRI_ROOT) -name "*.go")
	jiri go build -a -o $@/principal v.io/x/ref/cmd/principal
	jiri go build -a -tags wspr -o $@/servicerunner v.io/x/ref/cmd/servicerunner
	jiri go build -a -o $@/syncbased v.io/x/ref/services/syncbase/syncbased

.PHONY: gen-vdl
gen-vdl:
	jiri run vdl generate --lang=javascript --js-out-dir=src/gen-vdl v.io/v23/services/syncbase/...

node_modules: package.json
	npm prune
	npm install
	# Link Vanadium from JIRI_ROOT.
	rm -rf ./node_modules/vanadium
	cd "$(JIRI_ROOT)/release/javascript/core" && npm link
	npm link vanadium
	touch node_modules

# We use the same test runner as vanadium.js.  It handles starting and stopping
# all required services (proxy, wspr, mounntabled), and runs tests in chrome
# with prova.
# TODO(sadovsky): Some of the deps in our package.json are needed solely for
# runner.js. We should restructure things so that runner.js is its own npm
# package with its own deps.
.NOTPARALLEL: test
.PHONY: test
test: test-integration

.NOTPARALLEL: test-integration
.PHONY: test-integration
test-integration: test-integration-browser test-integration-node

.PHONY: test-integration-node
test-integration-node: export PATH := ./test:$(PATH)
test-integration-node: go/bin lint node_modules
	node ./node_modules/vanadium/test/integration/runner.js --services=start-syncbased.sh -- \
	prova test/integration/test-*.js $(PROVA_OPTS) $(NODE_OUTPUT_LOCAL)

.PHONY: test-integration-browser
test-integration-browser: export PATH := ./test:$(PATH)
test-integration-browser: go/bin lint node_modules
	node ./node_modules/vanadium/test/integration/runner.js --services=start-syncbased.sh -- \
	make test-integration-browser-runner

# Note: runner.js sets the V23_NAMESPACE and PROXY_ADDR env vars for the
# spawned test subprocess; we specify "make test-integration-browser-runner" as
# the test command so that we can then reference these vars in the Vanadium
# extension and our prova command.
.PHONY: test-integration-browser-runner
test-integration-browser-runner: VANADIUM_JS := $(JIRI_ROOT)/release/javascript/core
test-integration-browser-runner: BROWSER_OPTS := --options="--load-extension=$(VANADIUM_JS)/extension/build-test/,--ignore-certificate-errors,--enable-logging=stderr" $(BROWSER_OPTS)
test-integration-browser-runner:
	$(MAKE) -C $(VANADIUM_JS)/extension clean
	$(MAKE) -C $(VANADIUM_JS)/extension build-test
	prova ./test/integration/test-*.js $(PROVA_OPTS) $(BROWSER_OPTS) $(BROWSER_OUTPUT_LOCAL)

.PHONY: clean
clean:
	rm -rf \
		docs \
		go/bin \
		node_modules \
		tmp

.PHONY: lint
lint: node_modules
ifdef NOLINT
	@echo "Skipping lint - disabled by NOLINT environment variable"
else
	jshint .
endif

# The following target is copied and modifed from ../core/Makefile.
DOCSTRAP_LOC:= node_modules/ink-docstrap
JS_SRC_FILES = $(shell find src -name "*.js" | sed 's/ /\\ /')
docs: $(JS_SRC_FILES) ../core/jsdocs/docstrap-template/compiled/site.vanadium.css | node_modules
	# Copy our compiled style template
	cp -f ../core/jsdocs/docstrap-template/compiled/site.vanadium.css ${DOCSTRAP_LOC}/template/static/styles

	# Build the docs
	jsdoc $^ --readme ./jsdocs/index.md --configure ./jsdocs/conf.json --template ${DOCSTRAP_LOC}/template --destination $@

	# Copy favicon
	cp -f ../core/jsdocs/favicon.ico $@

# serve-docs
#
# Serve the docs at http://localhost:8020.
serve-docs: docs
	static docs -p 8020

.PHONY: serve-docs

.PHONY: deploy-docs-production
deploy-docs-production: docs
	make -C $(JIRI_ROOT)/infrastructure/deploy jsdoc-syncbase-production

.PHONY: deploy-docs-staging
deploy-docs-staging: docs
	make -C $(JIRI_ROOT)/infrastructure/deploy jsdoc-syncbase-staging
