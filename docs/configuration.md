# Configuration

Drune uses YAML files for configuration. There are two main types of configuration files:

- **Project File (`drune.yml`):** This file defines the project and its pipelines.
- **Pipeline File:** These files define the steps and settings for a single pipeline.

## Project File (`drune.yml`)

The `drune.yml` file is the main configuration file for your project. It should be placed in the root directory of your project.

**Example:**

```yaml
name: my_drune_project
version: 1.0

pipelines:
  - path: pipelines/my_pipeline.yml
```

**Keys:**

- `name` (required): The name of your project.
- `version` (required): The version of your project.
- `pipelines` (required): A list of paths to your pipeline configuration files.

## Pipeline File

Pipeline files define the steps and settings for a single pipeline.

**Example:**

```yaml
name: my_first_pipeline
engine: pandas

steps:
  - name: read_data
    type: reader
    format: csv
    path: data/input.csv

  - name: write_data
    type: writer
    format: csv
    path: data/output.csv
```

**Keys:**

- `name` (required): The name of the pipeline.
- `engine` (required): The processing engine to use for the pipeline. See [Engines](engines.md) for a list of supported engines.
- `steps` (required): A list of steps to execute in the pipeline. See [Pipelines](pipelines.md) for more information on steps.
