export DOCKER_BUILDKIT=1

.PHONY: sync
sync:
	@uv sync --all-extras --all-groups --all-packages -U

.PHONY: lint
lint:
	@uv run ruff check --fix

.PHONY: tc
tc:
	@uv run mypy .

.PHONY: pre
pre:
	@uv run pre-commit run --all-files

.PHONY: test
test:
	@uv run python -m pytest tests

.PHONY: testv
testv:
	@uv run python -m pytest -s tests

.PHONY: cov
cov:
	@uv run python -m pytest --cov=mint --cov-report=term-missing tests

.PHONY: check
check: lint tc test

.PHONY: next-version
next-version:
	@uvx --from python-semantic-release semantic-release --noop version --print
