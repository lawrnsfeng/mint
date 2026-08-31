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

# --- mint.worker: memory-capped test lanes ---
# Standing rule after the bug #15 OOM incident (see the mint.worker plan): any test
# whose independent variable is nesting depth / payload size runs once, standalone,
# under a memory cap before it ever joins the full suite. Container-backed tests
# (testcontainers) run one broker at a time for the same reason — never bundled.

.PHONY: test-worker
test-worker:
	@uv run python -m pytest tests/worker -k "not container and not integration"

.PHONY: test-worker-capped
test-worker-capped:
	@test -n "$(TARGET)" || (echo "usage: make test-worker-capped TARGET=<pytest target> [MEM=1G]" && exit 1)
	@systemd-run --user --scope -p MemoryMax=$(or $(MEM),1G) -- \
		.venv/bin/python -m pytest "$(TARGET)" -q

.PHONY: test-worker-containers
test-worker-containers:
	@$(MAKE) test-worker-capped TARGET=tests/worker/stores/test_redis_container.py MEM=1G
	@$(MAKE) test-worker-capped TARGET=tests/worker/brokers/test_redis_container.py MEM=1G
	@$(MAKE) test-worker-capped TARGET=tests/worker/brokers/test_rabbitmq_container.py MEM=1G
	@$(MAKE) test-worker-capped TARGET=tests/worker/brokers/test_kafka_container.py MEM=2G

.PHONY: check
check: lint tc test

.PHONY: next-version
next-version:
	@uvx --from python-semantic-release semantic-release --noop version --print
