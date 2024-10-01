
default: tests


.PHONY: init
init:
	pip install -r requirements.txt -U


.PHONY: pylint
pylint:
	@pylint --rcfile=pylintrc fsstratify


.PHONY: tests
tests: unit-tests


.PHONY: unit-tests
unit-tests:
	py.test -c pyproject.toml --cov


.PHONY: lnx-system-test
lnx-system-test:
# TODO: implement the actual test (currently it only runs the simulation)
	. .venv/bin/activate && \
	PATH=fsstratify/fsparsers/linux:$$PATH PYTHONPATH=. python fsstratify/__main__.py run tests/system/linux/
	@echo "==== STRATA =============="
	@cat tests/system/linux/simulation.strata
	@echo "=========================="
	#$(RM) tests/system/linux/simulation.log tests/system/linux/simulation.playbook tests/system/linux/simulation.strata


.PHONY: docs
docs:
	$(MAKE) -C docs/ html


.PHONY: serve-docs
serve-docs: docs
	@cd docs && sphinx-serve


.PHONY: proselint
proselint:
	@proselint -c docs/*.rst
