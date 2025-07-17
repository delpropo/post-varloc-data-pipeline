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
.PHONY: slurm_vembrane_tsv_to_zarr
slurm_vembrane_tsv_to_zarr:
	@read -p "Enter the name of the text file in the config folder (e.g., test_bcf_file.txt): " config_file; \
	if [ ! -f "config/$$config_file" ]; then \
		echo "File config/$$config_file does not exist!"; \
		exit 1; \
	fi; \
	echo "You entered config file: config/$$config_file"; \
	bash post_varloc_data_pipeline/slurm_vembrane_tsv_to_zarr.sh "$$(realpath config/$$config_file)"


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

## Extract ANN types from markdown and create dtype configuration files
.PHONY: extract_ann_types
extract_ann_types:
	$(PYTHON_INTERPRETER) post_varloc_data_pipeline/extract_ann_types.py --input references/ann_types.md --output-dir references
	@echo "ANN type extraction completed. Check config/ and references/ directories for generated files."




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
		sleep 15; \
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


## Test vembrane_tsv_to_zarr.py using expected.tsv and sample2.tsv
.PHONY: test_tsv_preprocess
test_tsv_preprocess:
	@echo "Testing vembrane_tsv_to_zarr.py on test files..."
	@mkdir -p tests/data/test_ann_all/zarr_output
	@echo "Step 1: Testing expected.tsv..."
	@jobid1=$$(sbatch --parsable --time=1:00:00 --cpus-per-task=1 --job-name=tsv_preprocess_expected \
		--output=tests/data/test_ann_all/tsv_preprocess_expected.slurm.out \
		--wrap="source ~/.bashrc && conda activate post-varloc-data-pipeline && python post_varloc_data_pipeline/vembrane_tsv_to_zarr.py --input tests/data/test_ann_all/expected.tsv --output tests/data/test_ann_all/zarr_output"); \
	echo "Submitted job $$jobid1 for expected.tsv, waiting for completion..."; \
	while squeue -j $$jobid1 2>/dev/null | grep -q $$jobid1; do \
		echo "Job $$jobid1 still running, waiting..."; \
		sleep 10; \
	done; \
	echo "Job $$jobid1 completed, checking results..."; \
	if [ ! -d "tests/data/test_ann_all/zarr_output/expected.zarr" ]; then \
		echo "✗ Test failed! expected.zarr file not created."; \
		exit 1; \
	fi; \
	echo "✓ expected.tsv test passed!"; \
	@echo "Step 2: Testing sample2.tsv..."; \
	@jobid2=$$(sbatch --parsable --time=1:00:00 --cpus-per-task=1 --job-name=tsv_preprocess_sample2 \
		--output=tests/data/test_ann_all/tsv_preprocess_sample2.slurm.out \
		--wrap="source ~/.bashrc && conda activate post-varloc-data-pipeline && python post_varloc_data_pipeline/vembrane_tsv_to_zarr.py --input tests/data/test_ann_all/sample2.tsv --output tests/data/test_ann_all/zarr_output"); \
	echo "Submitted job $$jobid2 for sample2.tsv, waiting for completion..."; \
	while squeue -j $$jobid2 2>/dev/null | grep -q $$jobid2; do \
		echo "Job $$jobid2 still running, waiting..."; \
		sleep 10; \
	done; \
	echo "Job $$jobid2 completed, checking results..."; \
	if [ ! -d "tests/data/test_ann_all/zarr_output/sample2.zarr" ]; then \
		echo "✗ Test failed! sample2.zarr file not created."; \
		exit 1; \
	fi; \
	echo "✓ sample2.tsv test passed!"; \
	echo "✓ All tsv_preprocess tests passed! Both Zarr files created successfully."

## Test zarr_pivot_creator.py using both expected.zarr and sample2.zarr with TSV output
.PHONY: test_zarr_pivot_creator
test_zarr_pivot_creator:
	@echo "Testing zarr_pivot_creator.py on test Zarr files with filtering and TSV export..."
	@if [ ! -d "tests/data/test_ann_all/zarr_output/expected.zarr" ] || [ ! -d "tests/data/test_ann_all/zarr_output/sample2.zarr" ]; then \
		echo "Error: Required zarr files not found. Run 'make test_tsv_preprocess' first."; \
		exit 1; \
	fi; \
	mkdir -p tests/data/test_ann_all/zarr_output/filtered
	@echo "Step 1: Testing expected.zarr filtering..."
	@jobid1=$$(sbatch --parsable --time=1:00:00 --cpus-per-task=1 --job-name=zarr_pivot_expected \
		--output=tests/data/test_ann_all/zarr_pivot_expected.slurm.out \
		--wrap="source ~/.bashrc && conda activate post-varloc-data-pipeline && python post_varloc_data_pipeline/zarr_pivot_creator.py --zarr tests/data/test_ann_all/zarr_output/expected.zarr --output tests/data/test_ann_all/zarr_output/filtered/expected_filtered_pivoted.zarr --export-tsv"); \
	echo "Submitted job $$jobid1 for expected.zarr, waiting for completion..."; \
	while squeue -j $$jobid1 2>/dev/null | grep -q $$jobid1; do \
		echo "Job $$jobid1 still running, waiting..."; \
		sleep 10; \
	done; \
	echo "Job $$jobid1 completed, checking results..."; \
	if [ ! -d "tests/data/test_ann_all/zarr_output/filtered/expected_filtered_pivoted.zarr" ]; then \
		echo "✗ Test failed! expected_filtered_pivoted.zarr not created."; \
		exit 1; \
	fi; \
	if [ ! -f "tests/data/test_ann_all/zarr_output/filtered/expected_filtered_pivoted.tsv" ]; then \
		echo "✗ Test failed! expected_filtered_pivoted.tsv not created."; \
		exit 1; \
	fi; \
	echo "✓ expected.zarr test passed!"; \
	@echo "Step 2: Testing sample2.zarr with ANN['MAX_AF'] filtering (preserving blank values)..."
	@jobid2=$$(sbatch --parsable --time=1:00:00 --cpus-per-task=1 --job-name=zarr_pivot_sample2 \
		--output=tests/data/test_ann_all/zarr_pivot_sample2.slurm.out \
		--wrap="source ~/.bashrc && conda activate post-varloc-data-pipeline && python post_varloc_data_pipeline/zarr_pivot_creator.py --zarr tests/data/test_ann_all/zarr_output/sample2.zarr --output tests/data/test_ann_all/zarr_output/filtered/sample2_filtered_pivoted.zarr --export-tsv"); \
	echo "Submitted job $$jobid2 for sample2.zarr, waiting for completion..."; \
	while squeue -j $$jobid2 2>/dev/null | grep -q $$jobid2; do \
		echo "Job $$jobid2 still running, waiting..."; \
		sleep 10; \
	done; \
	echo "Job $$jobid2 completed, checking results..."; \
	if [ ! -d "tests/data/test_ann_all/zarr_output/filtered/sample2_filtered_pivoted.zarr" ]; then \
		echo "✗ Test failed! sample2_filtered_pivoted.zarr not created."; \
		exit 1; \
	fi; \
	if [ ! -f "tests/data/test_ann_all/zarr_output/filtered/sample2_filtered_pivoted.tsv" ]; then \
		echo "✗ Test failed! sample2_filtered_pivoted.tsv not created."; \
		exit 1; \
	fi; \
	echo "✓ sample2.zarr test passed!"; \
	echo "✓ All zarr_pivot_creator tests passed! Both filtered Zarr and TSV files created successfully."


## Test zarr_groupby_aggregator.py using processed zarr files (first processes with zarr_pivot_creator, then aggregates)
.PHONY: test_zarr_groupby_aggregator
test_zarr_groupby_aggregator:
	@echo "Testing complete workflow: zarr_pivot_creator.py -> zarr_groupby_aggregator.py..."
	@if [ ! -d "tests/data/test_ann_all/zarr_output/expected.zarr" ]; then \
		echo "Error: expected.zarr not found. Run 'make test_tsv_preprocess' first."; \
		exit 1; \
	fi; \
	mkdir -p tests/data/test_ann_all/zarr_output/processed tests/data/test_ann_all/zarr_output/aggregated
	@echo "Step 1: Processing individual zarr files with zarr_pivot_creator.py..."
	@jobid1=$$(sbatch --parsable --time=1:00:00 --cpus-per-task=2 --job-name=zarr_pivot_step1 \
		--output=tests/data/test_ann_all/zarr_pivot_step1.slurm.out \
		--wrap="source ~/.bashrc && conda activate post-varloc-data-pipeline && python post_varloc_data_pipeline/zarr_pivot_creator.py --zarr tests/data/test_ann_all/zarr_output/expected.zarr --output tests/data/test_ann_all/zarr_output/processed/expected_processed.zarr && python post_varloc_data_pipeline/zarr_pivot_creator.py --zarr tests/data/test_ann_all/zarr_output/filtered/expected_filtered_pivoted.zarr --output tests/data/test_ann_all/zarr_output/processed/expected_filtered_processed.zarr"); \
	echo "Submitted processing job $$jobid1, waiting for completion..."; \
	while squeue -j $$jobid1 2>/dev/null | grep -q $$jobid1; do \
		echo "Processing job $$jobid1 still running, waiting..."; \
		sleep 10; \
	done; \
	echo "Step 2: Aggregating processed files with zarr_groupby_aggregator.py..."; \
	@jobid2=$$(sbatch --parsable --time=1:00:00 --cpus-per-task=4 --job-name=zarr_groupby_step2 \
		--output=tests/data/test_ann_all/zarr_groupby_step2.slurm.out \
		--wrap="source ~/.bashrc && conda activate post-varloc-data-pipeline && python post_varloc_data_pipeline/zarr_groupby_aggregator.py --zarr tests/data/test_ann_all/zarr_output/processed/expected_processed.zarr --output tests/data/test_ann_all/zarr_output/aggregated/grouped_results.tsv --workers 4"); \
	echo "Submitted aggregation job $$jobid2, waiting for completion..."; \
	while squeue -j $$jobid2 2>/dev/null | grep -q $$jobid2; do \
		echo "Aggregation job $$jobid2 still running, waiting..."; \
		sleep 10; \
	done; \
	echo "Jobs completed, checking results..."; \
	if [ -f "tests/data/test_ann_all/zarr_output/aggregated/grouped_results.tsv" ]; then \
		echo "✓ Test passed! Complete workflow successful - aggregated results file created."; \
		echo "Results preview:"; \
		head -n 5 tests/data/test_ann_all/zarr_output/aggregated/grouped_results.tsv; \
	else \
		echo "✗ Test failed! Aggregated results file not created."; \
		exit 1; \
	fi


## Run all tests and clean up afterwards
.PHONY: test_tests
test_tests: test_vembrane test_tsv_preprocess test_zarr_pivot_creator test_zarr_groupby_aggregator clean_tests
	@echo "All tests completed and cleaned up successfully!"

## Run all tests without cleanup (preserves test output files for inspection)
.PHONY: only_tests
only_tests: test_vembrane test_tsv_preprocess test_zarr_pivot_creator test_zarr_groupby_aggregator
	@echo "All tests completed! Test output files preserved for inspection."

## clean tests.  remove output tests/data/test_ann_all/output.tsv and tests/data/test_ann_all/zarr_output
.PHONY: clean_tests
clean_tests:
	@echo "Cleaning test output files..."
	@rm -f tests/data/test_ann_all/output.tsv
	@rm -f tests/data/test_ann_all/vembrane_table_test.slurm.out
	@rm -f tests/data/test_ann_all/tsv_preprocess_expected.slurm.out
	@rm -f tests/data/test_ann_all/tsv_preprocess_sample2.slurm.out
	@rm -f tests/data/test_ann_all/zarr_pivot_expected.slurm.out
	@rm -f tests/data/test_ann_all/zarr_pivot_sample2.slurm.out
	@rm -f tests/data/test_ann_all/zarr_pivot_step1.slurm.out
	@rm -f tests/data/test_ann_all/zarr_groupby_step2.slurm.out
	@rm -f tests/data/test_ann_all/zarr_groupby_test.slurm.out
	@rm -rf tests/data/test_ann_all/zarr_output
	@echo "Test output files cleaned."

## Create filtered and pivoted Zarr file(s) based on config.ini criteria
.PHONY: zarr_pivot_creator
zarr_pivot_creator:
	@read -p "Enter the path to a Zarr file or text file containing Zarr file paths: " input_path; \
	if [ ! -e "$$input_path" ]; then \
		echo "Error: $$input_path does not exist!"; \
		exit 1; \
	fi; \
	mkdir -p data/filtered; \
	if [ -f "$$input_path" ] && [ "$${input_path##*.}" = "txt" ]; then \
		echo "Processing Zarr files listed in: $$input_path"; \
		while IFS= read -r zarr_file || [ -n "$$zarr_file" ]; do \
			if [ -n "$$zarr_file" ] && [ -d "$$zarr_file" ]; then \
				echo "Creating filtered and pivoted Zarr for: $$zarr_file"; \
				$(PYTHON_INTERPRETER) post_varloc_data_pipeline/zarr_pivot_creator.py --zarr "$$zarr_file" --export-tsv; \
			elif [ -n "$$zarr_file" ]; then \
				echo "Warning: Zarr file $$zarr_file does not exist, skipping..."; \
			fi; \
		done < "$$input_path"; \
	elif [ -d "$$input_path" ]; then \
		echo "Creating filtered and pivoted Zarr for: $$input_path"; \
		$(PYTHON_INTERPRETER) post_varloc_data_pipeline/zarr_pivot_creator.py --zarr "$$input_path" --export-tsv; \
	else \
		echo "Error: $$input_path is neither a Zarr directory nor a .txt file!"; \
		exit 1; \
	fi

## Create filtered and pivoted Zarr file(s) using SLURM batch jobs
.PHONY: slurm_zarr_pivot_creator
slurm_zarr_pivot_creator:
	@read -p "Enter the name of the text file in the config folder (e.g., zarr_files.txt): " config_file; \
	if [ ! -f "config/$$config_file" ]; then \
		echo "File config/$$config_file does not exist!"; \
		exit 1; \
	fi; \
	input_path="$$(realpath config/$$config_file)"; \
	echo "You entered config file: config/$$config_file"; \
	slurm_account=$$(grep -E "^slurm_account\s*=" config.ini | cut -d'=' -f2 | tr -d ' '); \
	if [ -z "$$slurm_account" ]; then \
		echo "Warning: slurm_account not found in config.ini, proceeding without --account flag"; \
	else \
		echo "Using SLURM account: $$slurm_account"; \
	fi; \
	mkdir -p data/filtered logs; \
	if [ -f "$$input_path" ] && [ "$${input_path##*.}" = "txt" ]; then \
		echo "Processing Zarr files listed in: $$input_path"; \
		job_ids=(); \
		while IFS= read -r zarr_file || [ -n "$$zarr_file" ]; do \
			if [ -n "$$zarr_file" ] && [ -d "$$zarr_file" ]; then \
				echo "Submitting SLURM job for: $$zarr_file"; \
				job_name="zarr_pivot_$$(basename $$zarr_file .zarr)"; \
				log_file="logs/$${job_name}.slurm.out"; \
				if [ -n "$$slurm_account" ]; then \
					jobid=$$(sbatch --parsable --account="$$slurm_account" --time=2:00:00 --cpus-per-task=2 --mem-per-cpu=7G \
						--job-name="$$job_name" --output="$$log_file" \
						--wrap="source ~/.bashrc && conda activate $(PROJECT_NAME) && $(PYTHON_INTERPRETER) post_varloc_data_pipeline/zarr_pivot_creator.py --zarr '$$zarr_file' --export-tsv"); \
				else \
					jobid=$$(sbatch --parsable --time=2:00:00 --cpus-per-task=2 --mem-per-cpu=7G \
						--job-name="$$job_name" --output="$$log_file" \
						--wrap="source ~/.bashrc && conda activate $(PROJECT_NAME) && $(PYTHON_INTERPRETER) post_varloc_data_pipeline/zarr_pivot_creator.py --zarr '$$zarr_file' --export-tsv"); \
				fi; \
				job_ids+=($$jobid); \
				echo "  Submitted job $$jobid for $$zarr_file"; \
			elif [ -n "$$zarr_file" ]; then \
				echo "Warning: Zarr file $$zarr_file does not exist, skipping..."; \
			fi; \
		done < "$$input_path"; \
		if [ $${#job_ids[@]} -gt 0 ]; then \
			echo "Submitted $${#job_ids[@]} jobs. Monitoring completion..."; \
			for jobid in "$${job_ids[@]}"; do \
				echo "Waiting for job $$jobid to complete..."; \
				while squeue -j $$jobid 2>/dev/null | grep -q $$jobid; do \
					sleep 30; \
				done; \
				echo "Job $$jobid completed."; \
			done; \
			echo "All jobs completed successfully!"; \
		fi; \
	elif [ -d "$$input_path" ]; then \
		echo "Submitting SLURM job for: $$input_path"; \
		job_name="zarr_pivot_$$(basename $$input_path .zarr)"; \
		log_file="logs/$${job_name}.slurm.out"; \
		if [ -n "$$slurm_account" ]; then \
			jobid=$$(sbatch --parsable --account="$$slurm_account" --time=2:00:00 --cpus-per-task=2 --mem-per-cpu=7G \
				--job-name="$$job_name" --output="$$log_file" \
				--wrap="source ~/.bashrc && conda activate $(PROJECT_NAME) && $(PYTHON_INTERPRETER) post_varloc_data_pipeline/zarr_pivot_creator.py --zarr '$$input_path' --export-tsv"); \
		else \
			jobid=$$(sbatch --parsable --time=2:00:00 --cpus-per-task=2 --mem-per-cpu=7G \
				--job-name="$$job_name" --output="$$log_file" \
				--wrap="source ~/.bashrc && conda activate $(PROJECT_NAME) && $(PYTHON_INTERPRETER) post_varloc_data_pipeline/zarr_pivot_creator.py --zarr '$$input_path' --export-tsv"); \
		fi; \
		echo "Submitted job $$jobid for $$input_path"; \
		echo "Monitoring job completion..."; \
		while squeue -j $$jobid 2>/dev/null | grep -q $$jobid; do \
			echo "Job $$jobid still running, waiting..."; \
			sleep 30; \
		done; \
		echo "Job $$jobid completed successfully!"; \
	else \
		echo "Error: $$input_path is neither a Zarr directory nor a .txt file!"; \
		exit 1; \
	fi


## Run zarr_groupby_aggregator on user-specified processed files (expects files already processed by zarr_pivot_creator)
.PHONY: zarr_groupby_aggregator
zarr_groupby_aggregator:
	@read -p "Enter processed Zarr file paths (space-separated) or text file containing paths: " input; \
	read -p "Enter output file path (.tsv/.csv/.xlsx): " output; \
	if [ -f "$$input" ]; then \
		echo "Using file list: $$input"; \
		$(PYTHON_INTERPRETER) post_varloc_data_pipeline/zarr_groupby_aggregator.py --file-list "$$input" --output "$$output"; \
	else \
		echo "Using direct processed Zarr files: $$input"; \
		$(PYTHON_INTERPRETER) post_varloc_data_pipeline/zarr_groupby_aggregator.py --zarr $$input --output "$$output"; \
	fi

## Aggregate processed Zarr files using SLURM batch jobs (expects files already processed by zarr_pivot_creator)
.PHONY: slurm_zarr_groupby_aggregator
slurm_zarr_groupby_aggregator:
	@read -p "Enter the name of the text file in the config folder (e.g., processed_zarr_files.txt): " config_file; \
	if [ ! -f "config/$$config_file" ]; then \
		echo "File config/$$config_file does not exist!"; \
		exit 1; \
	fi; \
	input_path="$$(realpath config/$$config_file)"; \
	base_name="$$(basename $$config_file .txt)"; \
	output_path="data/aggregated/$${base_name}_aggregated.zarr"; \
	echo "You entered config file: config/$$config_file"; \
	echo "Output will be saved to: $$output_path"; \
	slurm_account=$$(grep -E "^slurm_account\s*=" config.ini | cut -d'=' -f2 | tr -d ' '); \
	if [ -z "$$slurm_account" ]; then \
		echo "Warning: slurm_account not found in config.ini, proceeding without --account flag"; \
	else \
		echo "Using SLURM account: $$slurm_account"; \
	fi; \
	mkdir -p data/aggregated logs; \
	echo "Submitting SLURM job for cross-file aggregation..."; \
	job_name="zarr_groupby_$${base_name}"; \
	log_file="logs/$${job_name}.slurm.out"; \
	if [ -n "$$slurm_account" ]; then \
		jobid=$$(sbatch --parsable --account="$$slurm_account" --time=14:00:00 --cpus-per-task=4 --mem-per-cpu=7G \
			--job-name="$$job_name" --output="$$log_file" \
			--wrap="source ~/.bashrc && conda activate $(PROJECT_NAME) && $(PYTHON_INTERPRETER) post_varloc_data_pipeline/zarr_groupby_aggregator.py --file-list '$$input_path' --output '$$output_path' --workers 4 --export-tsv"); \
	else \
		jobid=$$(sbatch --parsable --time=14:00:00 --cpus-per-task=4 --mem-per-cpu=7G \
			--job-name="$$job_name" --output="$$log_file" \
			--wrap="source ~/.bashrc && conda activate $(PROJECT_NAME) && $(PYTHON_INTERPRETER) post_varloc_data_pipeline/zarr_groupby_aggregator.py --file-list '$$input_path' --output '$$output_path' --workers 4 --export-tsv"); \
	fi; \
	echo "Submitted job $$jobid for aggregation of files listed in $$config_file"; \
	echo "Output will be: $$output_path (Zarr) and $$output_path.tsv (TSV export)"; \
	echo "Log file: $$log_file"; \
	echo "Monitoring job completion..."; \
	while squeue -j $$jobid 2>/dev/null | grep -q $$jobid; do \
		echo "Job $$jobid still running, waiting..."; \
		sleep 60; \
	done; \
	echo "Job $$jobid completed successfully!"; \
	echo "Getting job statistics..."; \
	my_job_statistics $$jobid; \
	if [ -d "$$output_path" ]; then \
		echo "✓ Aggregated Zarr file created: $$output_path"; \
		echo "File size: $$(du -sh $$output_path | cut -f1)"; \
	fi; \
	if [ -f "$${output_path%.zarr}.tsv" ]; then \
		echo "✓ TSV export created: $${output_path%.zarr}.tsv"; \
		echo "File size: $$(du -sh $${output_path%.zarr}.tsv | cut -f1)"; \
		echo "Preview of aggregated results:"; \
		head -n 3 "$${output_path%.zarr}.tsv"; \
	fi
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

