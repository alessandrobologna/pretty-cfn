.PHONY: examples examples-cfn examples-sam examples-cdk verify-examples help

REPO_ROOT := $(abspath $(CURDIR)/..)
RUN_IN_REPO := cd $(REPO_ROOT) &&

examples: examples-cfn examples-cdk
	@echo "All examples regenerated."

examples-cfn:
	@$(RUN_IN_REPO) for dir in examples/cfn/yaml/*/; do \
	  if [ -f "$$dir/input.yaml" ]; then \
	    echo "Formatting YAML $$(basename $$dir)"; \
	    uv run pretty-cfn format --input "$$dir/input.yaml" -o "$$dir/output.yaml"; \
	    echo "Generating SAM app for $$(basename $$dir)"; \
	    mkdir -p "$$dir/sam"; \
	    uv run pretty-cfn refactor --input "$$dir/input.yaml" --target sam-app -o "$$dir/sam/" --overwrite; \
	  fi; \
	done
	@$(RUN_IN_REPO) for dir in examples/cfn/json/*/; do \
	  if [ -f "$$dir/input.json" ]; then \
	    echo "Formatting JSON $$(basename $$dir)"; \
	    uv run pretty-cfn format --input "$$dir/input.json" -o "$$dir/output.yaml"; \
	    echo "Generating SAM app for $$(basename $$dir)"; \
	    mkdir -p "$$dir/sam"; \
	    uv run pretty-cfn refactor --input "$$dir/input.json" --target sam-app -o "$$dir/sam/" --overwrite; \
	  fi; \
	done

examples-cdk:
	@$(RUN_IN_REPO) for dir in examples/cdk/*/; do \
	  if ! command -v cdk >/dev/null 2>&1; then \
	    echo "cdk CLI not found on PATH for $$(basename $$dir); skipping"; \
	    continue; \
	  fi; \
	  echo "Synthesizing $$(basename $$dir)"; \
	  if ! (cd "$$dir" && rm -rf cdk.out && cdk synth >/dev/null); then \
	    echo "  cdk synth failed for $$(basename $$dir); skipping"; \
	    continue; \
	  fi; \
	  template=$$(ls "$$dir/cdk.out"/*.template.json 2>/dev/null | head -1); \
	  if [ -f "$$template" ]; then \
	    echo "Formatting and cleaning $$(basename $$dir)"; \
	    uv run pretty-cfn refactor --input "$$template" --target clean-cfn -o "$$dir/output.yaml"; \
	    echo "Generating SAM app for $$(basename $$dir)"; \
	    mkdir -p "$$dir/sam"; \
	    uv run pretty-cfn refactor --input "$$template" --target sam-app -o "$$dir/sam/" --overwrite; \
	  else \
	    echo "  No template.json artifact found for $$(basename $$dir); skipping"; \
	  fi; \
	done

verify-examples:
	@$(RUN_IN_REPO) uv run python examples/verify.py

help:
	@echo "Example targets (run from repo root via 'make -C examples <target>'):" \
	&& echo "  examples         - Regenerate all example outputs" \
	&& echo "  examples-cfn     - Regenerate CloudFormation example outputs" \
	&& echo "  examples-cdk     - Regenerate CDK example outputs" \
	&& echo "  verify-examples  - Validate example outputs with cfn-lint and idempotence checks"
