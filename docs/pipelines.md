# Pipelines

Pipelines are the core of Drune. They are defined in YAML files and consist of a series of steps that are executed in order.

## Steps

Steps are the building blocks of pipelines. Each step performs a specific action, such as reading data, transforming data, or writing data.

Drune provides a set of built-in step types, but you can also create your own custom step types.

### Common Step Keys

- `name` (required): The name of the step.
- `type` (required): The type of the step.

### Reader Step

The `reader` step is used to read data from a source.

**Example:**

```yaml
- name: read_data
  type: reader
  format: csv
  path: data/input.csv
```

**Keys:**

- `format` (required): The format of the data to read (e.g., `csv`, `json`, `parquet`).
- `path` (required): The path to the data to read.

### Writer Step

The `writer` step is used to write data to a destination.

**Example:**

```yaml
- name: write_data
  type: writer
  format: csv
  path: data/output.csv
```

**Keys:**

- `format` (required): The format to write the data in (e.g., `csv`, `json`, `parquet`).
- `path` (required): The path to write the data to.

### Transformer Step

The `transformer` step is used to apply transformations to the data.

**Example:**

```yaml
- name: transform_data
  type: transformer
  transformations:
    - type: select
      columns: ["id", "name"]
    - type: rename
      columns:
        id: customer_id
```

**Keys:**

- `transformations` (required): A list of transformations to apply.

### Validator Step

The `validator` step is used to validate the data against a set of quality rules.

**Example:**

```yaml
- name: validate_data
  type: validator
  rules:
    - type: not_null
      column: "id"
```

**Keys:**

- `rules` (required): A list of quality rules to apply.
