#################################################################################
# GLOBALS                                                                       #
#################################################################################

PROJECT_NAME = post-varloc-data-pipeline
PYTHON_VERSION = 3.10
PYTHON_INTERPRETER = python

#################################################################################
# COMMANDS                                                                      #
#################################################################################


## Install Python Dependencies.  If this fails, run requirements_failure
.PHONY: requirements
requirements:
	conda env update --name $(PROJECT_NAME) --file environment.yml --prune -v

## Install Python Dependencies
.PHONY: requirements_failure
requirements_failure:
	mamba env update --name $(PROJECT_NAME) --file environment.yml --prune -v

## Activate environment
.PHONY: activate
activate:
	echo "you must manually activate $(PROJECT_NAME)"
	# conda activate $(PROJECT_NAME)

## Delete all compiled Python files
.PHONY: clean
clean:
	find . -type f -name "*.py[co]" -delete
	find . -type d -name "__pycache__" -delete

## Lint using flake8 and black (use `make format` to do formatting)
.PHONY: lint
lint:
	flake8 post_varloc_data_pipeline
	isort --check --diff --profile black post_varloc_data_pipeline
	black --check --config pyproject.toml post_varloc_data_pipeline

## Format source code with black
.PHONY: format
format:
	black --config pyproject.toml post_varloc_data_pipeline




## Set up python interpreter environment
.PHONY: create_environment
create_environment:
	conda env create --name $(PROJECT_NAME) -f environment.yml

	@echo ">>> conda env created. Activate with:\nconda activate $(PROJECT_NAME)"




#################################################################################
# PROJECT RULES                                                                 #
#################################################################################


## Make Dataset
.PHONY: data
data: requirements
	@read -p "Enter the varlociraptor main folder or a specific tsv file path:" folder; \
	if [ -d "$$folder" ]; then \
		echo "Processing TSV files in $$folder"; \
	fi \
	&& $(PYTHON_INTERPRETER) post_varloc_data_pipeline/dataset.py --source $$folder \



## Make symlink
.PHONY: symlink
symlink:
	@read -p "Enter the varlociraptor main folder or a specific tsv file path:" folder; \
	if [ -d "$$folder" ]; then \
		echo "Processing TSV files in $$folder"; \
	fi \
	&& $(PYTHON_INTERPRETER) post_varloc_data_pipeline/dataset.py --source $$folder \



## Make zarr folder
.PHONY: nonslurm_zarr
nonslurm_zarr:
	@echo "Listing all files in data/raw folder:"; \
	for file in data/raw/*; do \
		echo $$file; \
		wc -l $$file; \
		readlink "$$file"; \
		echo "finished with file"; \
	done

## $(PYTHON_INTERPRETER) post_varloc_data_pipeline/tsv_to_zarr.py

## Make Dataset
.PHONY: clean_synlinks
clean_synlinks:
	@echo "Listing all files in data/raw folder:"; \
	for file in data/raw/*; do \
		echo $$file; \
		read -p "Do you want to delete this file? (y/n) " choice; \
		if [ "$$choice" = "y" ]; then \
			rm $$file; \
			echo "$$file deleted"; \
		else \
			echo "$$file kept"; \
		fi; \
	done

## run tsv_to_zarr.py using slurm
.PHONY: slurm_tsv_to_zarr
slurm_tsv_to_zarr:
	for file in data/raw/*; do \
		echo "raw data file: " $$file; \
		read -p "Do you want to create a zarr file from this file? (y/n) " choice; \
		if [ "$$choice" = "y" ]; then \
			echo "$(PYTHON_INTERPRETER) post_varloc_data_pipeline/tsv_to_zarr.py --input $$file"; \
		else \
			echo "$$file skipped"; \
		fi; \
	done

## run tsv_to_zarr.py using slurm
.PHONY: config_test
config_test:
	$(PYTHON_INTERPRETER) post_varloc_data_pipeline/config.py



## The goal is to create a symlink in the raw, make a zarr

#################################################################################
# Self Documenting Commands                                                     #
#################################################################################

.DEFAULT_GOAL := help

define PRINT_HELP_PYSCRIPT
import re, sys; \
lines = '\n'.join([line for line in sys.stdin]); \
matches = re.findall(r'\n## (.*)\n[\s\S]+?\n([a-zA-Z_-]+):', lines); \
print('Available rules:\n'); \
print('\n'.join(['{:25}{}'.format(*reversed(match)) for match in matches]))
endef
export PRINT_HELP_PYSCRIPT

help:
	@$(PYTHON_INTERPRETER) -c "${PRINT_HELP_PYSCRIPT}" < $(MAKEFILE_LIST)

