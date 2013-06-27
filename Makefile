REPORTER ?= dot

test-cov: lib-cov
	@NMQ_COV=1 $(MAKE) test REPORTER=html-cov > ./doc/coverage.html

lib-cov:
	@rm -fr ./$@
	@jscoverage lib $@

test:
	@./node_modules/.bin/mocha \
		--reporter $(REPORTER)

watch:
	@./node_modules/.bin/mocha \
		--watch \
		--reporter $(REPORTER)

clean:
	rm -fr lib-cov
	rm -f coverage.html

.PHONY: test