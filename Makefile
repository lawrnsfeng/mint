export DOCKER_BUILDKIT=1

.PHONY: sync
sync:
	@uv sync --all-extras --all-groups --all-packages -U
