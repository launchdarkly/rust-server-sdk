TEMP_TEST_OUTPUT=/tmp/contract-test-service.log
UNSUPPORTED_TESTS = -skip 'streaming/retry behavior' -skip 'streaming/validation/drop and reconnect if stream event has well-formed JSON not matching schema' -skip 'streaming/validation/drop and reconnect if stream event has malformed JSON' -skip 'evaluation/all flags state/client not ready' -skip 'evaluation/client not ready' -skip 'events/user properties/.*allAttributesPrivate=true.*/' -skip 'events/user properties/.*user-private.*/' -skip 'events/user properties/.*globally-private.*/' -skip 'evaluation/parameterized/evaluation failures.*/' -skip 'evaluation/parameterized/rule match.*/' -skip 'evaluation/all flags state/error in flag/' -skip 'events/feature events/full feature event for failed tracked flag/' -skip 'events/feature events/full feature event for tracked flag.*/'  -skip 'events/feature events/only index.*for untracked flag/' -skip 'data store/updates from stream' -skip 'evaluation/all flags state/compact representations'

build-contract-tests:
	@cargo build -p contract-tests --release

start-contract-test-service:
	@./target/release/contract-tests

start-contract-test-service-bg:
	@echo "Test service output will be captured in $(TEMP_TEST_OUTPUT)"
	@make start-contract-test-service >$(TEMP_TEST_OUTPUT) 2>&1 &

run-contract-tests:
	@curl -s https://raw.githubusercontent.com/launchdarkly/sdk-test-harness/master/downloader/run.sh \
      | VERSION=v1 PARAMS="-url http://localhost:8000 -debug -stop-service-at-end $(TEST_HARNESS_PARAMS) $(UNSUPPORTED_TESTS)" sh

contract-tests: build-contract-tests start-contract-test-service-bg run-contract-tests

.PHONY: build-contract-tests start-contract-test-service run-contract-tests contract-tests
