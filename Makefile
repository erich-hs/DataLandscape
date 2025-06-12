# Makefile for DataLandscape project

# Define directories
DAGS_DIR := dags
INCLUDE_DIR := include
LAMBDAS_DIR := lambdas

# Define Commands
RUFF_CHECK_CMD := uvx ruff check
RUFF_FORMAT_CMD := uvx ruff format
PYTEST_CMD := uv run pytest

# --- Generic Targets ---

# Lint a target directory
lint:
	@echo "Linting $(TARGET_DIR)..."
	$(RUFF_CHECK_CMD) $(TARGET_DIR)

# Format a target directory
format:
	@echo "Formatting $(TARGET_DIR)..."
	$(RUFF_FORMAT_CMD) $(TARGET_DIR)

# Test a target directory
test:
	@echo "Testing $(TARGET_DIR)..."
	$(PYTEST_CMD) $(TARGET_DIR)

# --- DAGs Targets ---

lint-dags:
	$(MAKE) lint TARGET_DIR=$(DAGS_DIR)

format-dags:
	$(MAKE) format TARGET_DIR=$(DAGS_DIR)

test-dags:
	$(MAKE) test TARGET_DIR=$(DAGS_DIR)

# --- Include Targets ---

lint-include:
	$(MAKE) lint TARGET_DIR=$(INCLUDE_DIR)

format-include:
	$(MAKE) format TARGET_DIR=$(INCLUDE_DIR)

test-include:
	$(MAKE) test TARGET_DIR=$(INCLUDE_DIR)

# --- Lambdas Targets ---

lint-lambdas:
	$(MAKE) lint TARGET_DIR=$(LAMBDAS_DIR)

format-lambdas:
	$(MAKE) format TARGET_DIR=$(LAMBDAS_DIR)

test-lambdas:
	@for lambda_dir in $$(find $(LAMBDAS_DIR) -mindepth 1 -maxdepth 1 -type d); do \
		if [ -d "$$lambda_dir/tests" ]; then \
			echo "--- Testing $$lambda_dir ---"; \
			req_file="$$lambda_dir/requirements.txt"; \
			if [ -f "$$req_file" ]; then \
				uv run --isolated --with-requirements $$req_file --with-requirements requirements-dev.txt pytest $$lambda_dir; \
			else \
				echo "  No requirements.txt found for $$lambda_dir. Using only dev requirements."; \
				uv run --isolated --with-requirements requirements-dev.txt pytest $$lambda_dir; \
			fi; \
		else \
			echo "--- Skipping $$lambda_dir (no tests directory) ---"; \
		fi \
	done
	@echo "--- All lambda tests concluded ---"

# --- All Targets ---

lint-all: lint-dags lint-include lint-lambdas

format-all: format-dags format-include format-lambdas

test-all: test-dags test-include test-lambdas

all: lint-all format-all test-all 