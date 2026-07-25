.PHONY: memoree/cache-size memoree/clean-dev memoree/watch

memoree/cache-size:
	@./scripts/dev-cache.sh size

memoree/clean-dev:
	@./scripts/dev-cache.sh clean

memoree/watch:
	@./scripts/dev-cache.sh watch $(WATCH_ARGS)
