.PHONY: test

gofmt:
	./scripts/gofmt.sh

test:
	./scripts/run-tests.sh
