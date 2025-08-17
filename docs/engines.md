# Engines

Drune supports multiple data processing engines. You can specify the engine to use for a pipeline in the pipeline configuration file.

## Supported Engines

- **Pandas:** The default engine. It is a good choice for small to medium-sized datasets that can fit in memory.
- **Spark:** A distributed processing engine that can handle large datasets.
- **DuckDB:** An in-process analytical database that is a good choice for medium-sized datasets.
- **Polars:** A fast, multi-threaded DataFrame library written in Rust.

## Engine Configuration

To use a specific engine, you need to install the corresponding extra:

```bash
pip install drune[spark]
pip install drune[duckdb]
pip install drune[pandas]
pip install drune[polars]
```

Then, you can specify the engine in your pipeline configuration file:

```yaml
name: my_pipeline
engine: spark

steps:
  ...
```
