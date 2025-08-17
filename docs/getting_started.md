# Getting Started

This guide will walk you through the process of installing, configuring, and running your first `drune` pipeline.

## Installation

Drune is available on PyPI and can be installed using pip:

```bash
pip install drune
```

To install with support for a specific engine, you can use the following extras:

```bash
pip install drune[spark]
pip install drune[duckdb]
pip install drune[pandas]
pip install drune[polars]
```

## Your First Pipeline

1.  **Create a project directory:**

    ```bash
    mkdir my-drune-project
    cd my-drune-project
    ```

2.  **Create a `drune.yml` file:**

    This file defines your project and the pipelines it contains.

    ```yaml
    name: my_drune_project
    version: 1.0

    pipelines:
      - path: pipelines/my_pipeline.yml
    ```

3.  **Create your pipeline configuration file:**

    Create a directory called `pipelines` and a file named `my_pipeline.yml` inside it.

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

4.  **Create your data file:**

    Create a directory called `data` and a file named `input.csv` inside it.

    ```csv
    id,name
    1,John Doe
    2,Jane Doe
    ```

5.  **Run the pipeline:**

    ```bash
    drune run
    ```

After running the pipeline, you should see a new file called `output.csv` in the `data` directory.
