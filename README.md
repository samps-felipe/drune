# Drune

[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/drune)](https://pypi.org/project/drune/)
[![PyPI](https://img.shields.io/pypi/v/drune)](https://pypi.org/project/drune/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)

## 🚀 Overview

**Drune** is a powerful and flexible Python library designed to streamline and standardize the development of data pipelines (ETL/ELT). By adopting a declarative approach, it allows users to define complex data transformations, data quality rules, and data loading strategies using simple, human-readable YAML configuration files, eliminating the need for repetitive boilerplate code.

This framework is particularly well-suited for environments like Databricks, but its modular design supports various data processing engines, making it adaptable to different data ecosystems.

### ✨ Problem Solved

Traditional data pipeline development often involves writing extensive code (e.g., PySpark, SQL) for each step, leading to:
*   **Repetitive Code:** Similar patterns for reading, transforming, and writing data are re-implemented across multiple pipelines.
*   **Lack of Standardization:** Inconsistent coding practices can lead to maintainability challenges and increased debugging time.
*   **Complex Data Quality:** Implementing robust data quality checks can be cumbersome and error-prone.
*   **Engine-Specific Logic:** Tightly coupled code to a specific processing engine (e.g., Spark) makes migration difficult.

The Drune addresses these challenges by providing a high-level abstraction layer, allowing data engineers and analysts to focus on *what* data operations need to be performed rather than *how* they are executed.

### 🌟 Key Advantages

*   **Declarative Configuration (YAML):** Define entire data pipelines using intuitive YAML files, enhancing readability, maintainability, and collaboration.
*   **Multi-Engine Support:** Seamlessly switch between different data processing engines (e.g., Spark, Pandas, Polars, DuckDB, Postgres) by simply changing a configuration parameter. This promotes portability and future-proofing.
*   **Medallion Architecture Ready:** Built-in support for common data layering patterns (e.g., Silver for cleaning/standardization, Gold for aggregation/business views), facilitating structured data lake development.
*   **Integrated Data Quality:** Define data validation rules (constraints) directly within your YAML configurations, ensuring data integrity and reliability from ingestion to consumption.
*   **Extensible and Modular:** Easily extend the framework by adding custom transformation steps, data quality rules, or even new processing engines to fit specific project requirements.
*   **Accelerated Development:** Drastically reduces development time by abstracting away low-level implementation details, allowing teams to build and deploy pipelines faster.

## 📦 Installation

### Prerequisites

*   Python 3.8 or higher.
*   `pip` (Python package installer).

### Install from PyPI

The easiest way to install the Drune is via pip:

```bash
pip install drune
```

### Install from Source (for Development)

If you plan to contribute to the project or need the latest development version, you can install it from source:

```bash
git clone https://github.com/your-repo/drune.git # Replace with actual repo URL
cd drune
pip install -e .
```

## 🚀 Usage

The Drune operates by reading a YAML configuration file that describes your data pipeline. It then uses the specified engine to execute the defined steps.

### CLI (Command Line Interface)

The framework provides a command-line interface for executing pipelines.

```bash
drune --help
```

### YAML Configuration Structure

A pipeline is defined in a YAML file. The structure varies slightly depending on the chosen `engine`.

#### Example: Silver Pipeline

A Silver pipeline is typically used for data ingestion, cleaning, typing, standardization, and initial validation of raw data.

```yaml
# exemplo.yaml (or pipelines/silver_accounts.yaml)
engine: "pandas" # or "spark", "polars", "duckdb", "postgres"
pipeline_type: "silver"
pipeline_name: "silver_accounts"
description: "Ingests account data, applies quality and standardization."

source:
  type: "csv"
  path: "dados/exemplo.csv" # Relative path to your data file
  options:
    header: true
    sep: ","

sink:
  type: "csv"
  path: "dados/saida/teste.csv" # Output path
  mode: "overwrite" # or "append"

columns:
  - name: "id"
    rename: "account_id"
    type: "int"
    description: "Unique identifier for the account."
    pk: true # Informative: indicates primary key
    validate:
      - "not_null"
      - "unique"

  - name: "name"
    rename: "account_name"
    type: "string"
    transform: "UPPER(TRIM(name))" # SQL-like expression for transformation
    validate:
      - "not_null"
      - "min_length:3"

  - name: "balance"
    type: "float"
    validate:
      - "greater_than_or_equal_to:0"
      - "less_than_or_equal_to:1000000"

  - name: "created_at"
    type: "datetime"
    validate:
      - "is_date"
```

#### Example: Gold Pipeline

A Gold pipeline is used to create aggregated tables and business views, often joining data from one or more Silver layer tables.

```yaml
# pipelines/gold_enriched_customers.yaml
engine: "spark" # or "dlt" (if using Databricks DLT generation)
pipeline_type: "gold"
pipeline_name: "gold_enriched_customers"
description: "Creates a 360-degree view of customers with account data."

dependencies: # Tables from the Silver layer required for this pipeline
  - "dev.silver.customers"
  - "dev.silver.accounts"

sink:
  catalog: "dev"
  schema: "gold"
  table: "enriched_customers"
  mode: "overwrite"

transformation:
  type: "sql" # Currently, SQL is the primary transformation type for Gold
  sql: |
    SELECT
      c.customer_id,
      c.full_name,
      COUNT(a.account_id) AS num_accounts,
      SUM(a.balance) AS total_balance
    FROM dev.silver.customers c
    LEFT JOIN dev.silver.accounts a ON c.customer_id = a.customer_id
    GROUP BY c.customer_id, c.full_name
```

### Running a Pipeline

To execute a pipeline defined in a YAML file, use the CLI:

```bash
drune run --config-path exemplo.yaml
```

Replace `exemplo.yaml` with the actual path to your pipeline configuration file.

## 🧩 Core Concepts

### Engines

The framework supports multiple data processing engines, allowing you to choose the best tool for your specific needs. Each engine provides an implementation of the core pipeline steps.

*   **Spark:** Ideal for large-scale data processing, especially within Databricks or other Spark environments.
*   **Pandas:** Excellent for smaller to medium-sized datasets, local development, and quick prototyping.
*   **Polars:** A high-performance DataFrame library written in Rust, offering speed and memory efficiency.
*   **DuckDB:** An in-process SQL OLAP database, great for analytical queries on local files.
*   **Postgres:** For interacting with PostgreSQL databases.

### Pipelines

Pipelines define the flow of data processing. The framework currently supports:

*   **Silver Pipelines:** Focus on data cleansing, standardization, type conversion, and initial data quality checks. They typically read from raw sources and write to a cleaned, structured layer.
*   **Gold Pipelines:** Focus on data aggregation, enrichment, and creating business-ready views. They typically read from Silver or other curated layers and write to a consumption layer.

### Steps

Each pipeline execution is broken down into a series of steps, which are implemented differently by each engine:

*   **Reader:** Reads data from a specified source (e.g., CSV, Parquet, database table).
*   **Transformer:** Applies transformations to the data (e.g., column renaming, type casting, custom expressions).
*   **Validator:** Executes data quality rules and identifies invalid records.
*   **Writer:** Writes the processed data to a specified destination.
*   **Tester:** (Specific to some engines/contexts) Used for testing configurations or data.

### Data Quality

Data quality is a first-class citizen in this framework. You can define validation rules directly in your YAML configuration for each column. The framework applies these rules during pipeline execution, providing insights into data integrity.

Examples of validation rules: `not_null`, `unique`, `min_length`, `is_date`, `greater_than_or_equal_to`, `isin`.

### Catalog

The catalog concept helps manage data assets (tables, schemas) across different environments and engines, providing a unified way to reference data sources and sinks.

## 🛠️ Extensibility

The Drune is designed to be highly extensible, allowing you to tailor it to your specific requirements without modifying the core library code.

### Adding Custom Validation Rules

You can register your own custom validation rules to be used in your YAML configurations.

```python
# In your project's custom code (e.g., my_custom_rules.py)
from declarative_data_framework.core.quality import register_rule
from declarative_data_framework.models.pydantic_models import Column

class MyCustomValidation:
    def __init__(self, params: dict = None):
        self.params = params or {}

    def apply(self, df, column_config: Column):
        column_name = column_config.name
        # Example: Check if all values in a column are even
        # This is a simplified example; actual implementation would vary by engine
        print(f"Applying custom rule 'my_custom_even_check' on column '{column_name}'")
        # In a real scenario, you'd return (failures_df, success_df)
        # For demonstration, let's assume no failures for now
        return df.limit(0), df # Return empty failures, all successes for example

# Register your rule (overwrites if the name already exists)
register_rule("my_custom_even_check", MyCustomValidation)

# You can also use it as a decorator:
@register_rule("another_custom_rule")
class AnotherCustomValidation:
    def __init__(self, params: dict = None):
        self.params = params or {}
    def apply(self, df, column_config: Column):
        # Your validation logic here
        return df.limit(0), df
```

To make your custom rules available, ensure your application imports the file where they are registered before running the pipeline.

### Adding Custom Steps/Transformations

For engines like Spark or Pandas, you can extend their capabilities by adding new transformation logic. This typically involves modifying the respective engine's step handlers (e.g., `src/declarative_data_framework/engines/spark/steps/transformer.py` or `src/declarative_data_framework/engines/pandas/steps/transformer.py`).

### Adding New Engines

The framework's abstract `BaseEngine` class (`src/declarative_data_framework/core/engine.py`) provides a contract for integrating new data processing engines. To add a new engine, you would:
1.  Create a new module under `src/declarative_data_framework/engines/` (e.g., `src/declarative_data_framework/engines/my_new_engine/`).
2.  Implement a class that inherits from `BaseEngine` and implements all its abstract methods.
3.  Register your new engine using the `@register_engine` decorator.

## 🤝 Development & Contribution

We welcome contributions to the Drune!

### Setting Up Your Development Environment

1.  **Fork the repository:** Go to the project's GitHub page and click "Fork".
2.  **Clone your fork:**
    ```bash
    git clone https://github.com/your-repo/drune.git
    cd drune
    ```
3.  **Create a virtual environment:**
    ```bash
    python -m venv .venv
    source .venv/bin/activate # On Windows: .venv\Scripts\activate
    ```
4.  **Install dependencies in editable mode:**
    ```bash
    pip install -e .
    pip install -r requirements-dev.txt # If a dev requirements file exists
    ```

### Running Tests

The project uses `pytest` for testing. To run the tests:

```bash
pytest
```

### Contribution Guidelines

1.  **Branching:** Create a new branch for your feature or bug fix: `git checkout -b feature/your-feature-name` or `bugfix/your-bug-fix`.
2.  **Code Style:** Adhere to PEP 8 and existing code style. Use a linter (e.g., `ruff`, `flake8`) and formatter (e.g., `black`) if configured in the project.
3.  **Tests:** Write unit and integration tests for your changes to ensure functionality and prevent regressions.
4.  **Documentation:** Update the `README.md` or other relevant documentation for any new features or significant changes.
5.  **Commit Messages:** Write clear and concise commit messages.
6.  **Pull Requests:** Submit a pull request to the `main` branch of the original repository. Provide a detailed description of your changes.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
