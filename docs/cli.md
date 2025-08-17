# Command-Line Interface (CLI)

Drune provides a command-line interface (CLI) for running pipelines and managing projects.

## `run`

The `run` command is used to run a pipeline.

```bash
drune run [OPTIONS]
```

**Options:**

- `--pipeline TEXT`: The name of the pipeline to run.
- `--project-file TEXT`: The path to the project file.

## `validate`

The `validate` command is used to validate a pipeline.

```bash
drune validate [OPTIONS]
```

**Options:**

- `--pipeline TEXT`: The name of the pipeline to validate.
- `--project-file TEXT`: The path to the project file.

## `init`

The `init` command is used to create a new Drune project.

```bash
drune init [OPTIONS] NAME
```

**Arguments:**

- `NAME`: The name of the project to create.
