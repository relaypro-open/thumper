.PHONY: all
all:
	@(rebar3 compile)

.PHONY: shell
shell:
	@(rebar3 shell)

.PHONY: test
test:
	@(rebar3 eunit skip_deps=true)
