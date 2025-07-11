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

## Install Python Dependencies to be run if there is a failure.
.PHONY: requirements_failure
requirements_failure:
	srun  --partition=standard --pty -c 1 --mem=4g --time=00:30:00 bash -c "conda env update --name $(PROJECT_NAME) --file environment.yml --prune -v"
	# works if failue was due to lack of memory on login node.

## Install Python Dependencies to be run if there is a failure.
.PHONY: mamba_requirements_failure
mamba_requirements_failure:
	srun  --partition=standard --pty -c 1 --mem=14g --time=00:30:00 bash -c "mamba env update --name $(PROJECT_NAME) --file environment.yml --prune -v"
	# works if failue was due to lack of memory on login node.

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
## Make TSV files from bcf or vcf
.PHONY: slurm_create_tsv
create_tsv:
	@read -p "Enter the name of the text file in the config folder (e.g., test_bcf_file.txt): " config_file; \
	if [ ! -f "config/$$config_file" ]; then \
		echo "File config/$$config_file does not exist!"; \
		exit 1; \
	fi; \
	echo "You entered config file: config/$$config_file"; \
	bash post_varloc_data_pipeline/slurm_vembrane_processing.sh "$$(realpath config/$$config_file)"

## Preprocess TSV files to Zarr using slurm
.PHONY: slurm_tsv_preprocess_to_zarr
slurm_tsv_preprocess_to_zarr:
	@read -p "Enter the name of the text file in the config folder (e.g., test_bcf_file.txt): " config_file; \
	if [ ! -f "config/$$config_file" ]; then \
		echo "File config/$$config_file does not exist!"; \
		exit 1; \
	fi; \
	echo "You entered config file: config/$$config_file"; \
	bash post_varloc_data_pipeline/slurm_tsv_preprocess_to_zarr.sh "$$(realpath config/$$config_file)"


## Make Dataset
.PHONY: data
data: requirements
	@read -p "Enter the varlociraptor main folder or a specific tsv file path:" folder; \
	if [ -d "$$folder" ]; then \
		echo "Processing TSV files in $$folder"; \
	fi \
	&& $(PYTHON_INTERPRETER) post_varloc_data_pipeline/dataset.py --source $$folder \







## run tsv_to_zarr.py using slurm
.PHONY: tsv_to_zarr
tsv_to_zarr:
	for file in data/raw/*; do \
		echo "raw data file: " $$file; \
		read -p "Do you want to create a zarr file from this file? (y/n) " choice; \
		if [ "$$choice" = "y" ]; then \
			$(PYTHON_INTERPRETER) post_varloc_data_pipeline/tsv_to_zarr.py --input $$file; \
		else \
			echo "$$file skipped"; \
		fi; \
	done






## run tsv_to_zarr.py using slurm
.PHONY: config_test
config_test:
	$(PYTHON_INTERPRETER) post_varloc_data_pipeline/config.py





## Test vembrane on tests/data/test_ann_all/test.vcf by running vembrane table ALL and compare output to expected.tsv
.PHONY: test_vembrane
test_vembrane:
	@echo "Running vembrane table ALL on tests/data/test_ann_all/test.vcf..."
	@jobid=$$(sbatch --parsable --time=1:00:00 --cpus-per-task=1 --job-name=vembrane_table_test \
		--output=tests/data/test_ann_all/vembrane_table_test.slurm.out \
		--wrap="source ~/.bashrc && conda activate post-varloc-data-pipeline && vembrane table ALL tests/data/test_ann_all/test.vcf > tests/data/test_ann_all/output.tsv"); \
	echo "Submitted job $$jobid, waiting for completion..."; \
	while squeue -j $$jobid 2>/dev/null | grep -q $$jobid; do \
		echo "Job $$jobid still running, waiting..."; \
		sleep 10; \
	done; \
	echo "Job $$jobid completed, checking results..."; \
	if [ -f "tests/data/test_ann_all/output.tsv" ]; then \
		if diff tests/data/test_ann_all/output.tsv tests/data/test_ann_all/expected.tsv > /dev/null 2>&1; then \
			echo 'Test passed!'; \
		else \
			echo 'Test failed! Files differ.'; \
			exit 1; \
		fi; \
	else \
		echo "Test failed! output.tsv was not created."; \
		exit 1; \
	fi


## Test tsv_preprocess_to_zarr.py using expected.tsv
.PHONY: test_tsv_preprocess
test_tsv_preprocess:
	@echo "Testing tsv_preprocess_to_zarr.py on tests/data/test_ann_all/expected.tsv..."
	@mkdir -p tests/data/test_ann_all/zarr_output
	@jobid=$$(sbatch --parsable --time=1:00:00 --cpus-per-task=1 --job-name=tsv_preprocess_test \
		--output=tests/data/test_ann_all/tsv_preprocess_test.slurm.out \
		--wrap="source ~/.bashrc && conda activate post-varloc-data-pipeline && python post_varloc_data_pipeline/tsv_preprocess_to_zarr.py --input tests/data/test_ann_all/expected.tsv --output tests/data/test_ann_all/zarr_output"); \
	echo "Submitted job $$jobid, waiting for completion..."; \
	while squeue -j $$jobid 2>/dev/null | grep -q $$jobid; do \
		echo "Job $$jobid still running, waiting..."; \
		sleep 10; \
	done; \
	echo "Job $$jobid completed, checking results..."; \
	if [ -d "tests/data/test_ann_all/zarr_output/expected.zarr" ]; then \
		echo "Test passed! Zarr file created successfully."; \
	else \
		echo "Test failed! Zarr file not created."; \
		exit 1; \
	fi


## Run all tests and clean up afterwards
.PHONY: test_tests
test_tests: test_vembrane test_tsv_preprocess clean_tests
	@echo "All tests completed and cleaned up successfully!"

## clean tests.  remove output tests/data/test_ann_all/output.tsv and tests/data/test_ann_all/zarr_output
.PHONY: clean_tests
clean_tests:
	@echo "Cleaning test output files..."
	@rm -f tests/data/test_ann_all/output.tsv
	@rm -f tests/data/test_ann_all/vembrane_table_test.slurm.out
	@rm -f tests/data/test_ann_all/tsv_preprocess_test.slurm.out
	@rm -rf tests/data/test_ann_all/zarr_output
	@echo "Test output files cleaned."


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

