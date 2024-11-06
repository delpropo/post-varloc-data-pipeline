
## The `contrib` Folder in a Git Repository

The `contrib` folder in a Git repository is typically used to store contributions that are not part of the core functionality of the project but are still useful for users or developers. These contributions can include scripts, tools, documentation, or any other resources that enhance the usability or development process of the project. The contents of the `contrib` folder are often community-driven and can vary widely depending on the nature of the project.

### Common Contents of the `contrib` Folder

- **Scripts**: Utility scripts for building, testing, or deploying the project.
- **Documentation**: Additional documentation or examples that help users understand how to use the project.
- **Configuration Files**: Sample configuration files that can be used as templates.
- **Tools**: Auxiliary tools that assist in the development or usage of the project.

By organizing these contributions in a dedicated folder, the project maintains a clean structure while still providing valuable resources to its users and contributors.

















## Slurm Job Management

```bash
#!/bin/bash

#sbatch       --account=margit0
#sbatch       --partition=standard
#sbatch       -c 1
#sbatch       --nodes=1
#sbatch       --mem-per-cpu=7g
#sbatch       --time=00-00:02:00
```

## Understanding Job Numbers in Slurm

In Slurm, each job is assigned a unique job number (or job ID) when it is submitted. This job number is crucial for managing and tracking jobs. Here’s what you need to know about job numbers:

- **Uniqueness**: Each job number is unique and is used to identify a specific job within the Slurm system.
- **Usage**: Job numbers are used in various Slurm commands to perform actions on specific jobs. For example:
    - To check the status of a job:
        ```bash
        squeue -j <job_id>
        ```
    - To cancel a job:
        ```bash
        scancel <job_id>
        ```
    - To view detailed information about a job:
        ```bash
        scontrol show job <job_id>
        ```
- **Format**: Job numbers are typically integers and are assigned sequentially by the Slurm controller.
- **Persistence**: Job numbers remain associated with a job for its entire lifecycle, from submission to completion.

Understanding and using job numbers effectively can help you manage your jobs more efficiently in a Slurm-managed cluster.

## Useful Slurm Commands

Here are some useful Slurm commands that can help you manage your jobs:

- `squeue` (alias `sq`):
    - **Description**: Displays information about jobs located in the Slurm scheduling queue.
    - **Usage**:
        ```bash
        squeue
        ```
    - **Example**:
        ```bash
        squeue -u username
        ```
    - **Alias Example**:
        ```bash
        sq -u username
        ```

- `scancel`:
    - **Description**: Cancels a pending or running job.
    - **Usage**:
        ```bash
        scancel <job_id>
        ```
    - **Example**:
        ```bash
        scancel 12345
        ```

- `sbatch`:
    - **Description**: Submits a batch script to the Slurm scheduler.
    - **Usage**:
        ```bash
        sbatch script.sh
        ```

- `sinfo`:
    - **Description**: Displays information about Slurm nodes and partitions.
    - **Usage**:
        ```bash
        sinfo
        ```

- `scontrol`:
    - **Description**: Used to view and modify Slurm configuration and state.
    - **Usage**:
        ```bash
        scontrol show job <job_id>
        ```

## Common Issues and Solutions

### Environment Activation

One common issue when submitting jobs in Slurm is that the environment is not activated by default. This means that any environment-specific settings or dependencies will not be available unless explicitly activated within the job script. To address this, you should create a secondary script that activates the environment and executes the desired program.

Here’s a step-by-step guide to handle this:

1. **Create a Secondary Script**: This script will activate the environment and run your program.
    ```bash
    #!/bin/bash
    source /path/to/your/environment/bin/activate
    python your_program.py
    ```

2. **Modify Your Job Script**: Update your job script to call the secondary script.
    ```bash
    #!/bin/bash
    #SBATCH --account=margit0
    #SBATCH --partition=standard
    #SBATCH -c 1
    #SBATCH --nodes=1
    #SBATCH --mem-per-cpu=7g
    #SBATCH --time=00-00:02:00

    srun secondary_script.sh
    ```

By following these steps, you ensure that the necessary environment is activated, and your program runs correctly within the Slurm job.

https://github.com/um-dang/conda_on_the_cluster




# getting proxy to work on a cluster.
At this time, there is a requirement to use a different openai_api_base on the cluster.
openai_api_base="https://umichai.azure-api.net/azure-openai-api"
as opposed to
openai_api_base="https://api.umgpt.umich.edu/azure-openai-api"


The NO_PROXY environment variable is used to exclude specific domains from the proxy settings. For the cluster, it excludes .umich.edu domains

CNAME might change in the future so if it breaks you want to run nslookup api.umgpt.umich.edu  and see what it was updated to

```bash

nslookup api.umgpt.umich.edu
nslookup umichai.azure-api.net
```