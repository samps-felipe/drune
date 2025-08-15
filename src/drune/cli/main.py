import yaml
import click
from drune.core.pipeline import Pipeline
from drune.engines.pandas.pandas_engine import PandasEngine
from drune.models.pydantic_models import PipelineConfig


def get_engine(engine_name: str, config):
    """Factory to instantiate the correct engine based on config."""
    if engine_name == 'pandas':
        # SparkEngine should handle its own SparkSession creation
        return PandasEngine(config)
    else:
        raise ValueError(f"Engine '{engine_name}' not supported.")


def _load_and_validate_config(config_path: str):
    """Load and validate the pipeline configuration from a YAML file."""
    with open(config_path, 'r') as f:
        config_dict = yaml.safe_load(f)
    try:
        config = PipelineConfig(**config_dict)
    except Exception as e:
        raise ValueError(f"YAML validation error: {e}")
    return config


@click.group()
def cli():
    """Drune CLI"""
    pass


@cli.command()
@click.argument('config_path', type=click.Path(exists=True))
def run(config_path: str):
    """Run the pipeline using the provided YAML configuration."""
    config = _load_and_validate_config(config_path)
    engine = get_engine(config.engine, config)
    pipeline = Pipeline(config, engine)
    pipeline.run()
    click.echo(f"Pipeline '{config.pipeline_name}' executed successfully.")


@cli.command()
@click.argument('config_path', type=click.Path(exists=True))
def create(config_path: str):
    """Create pipeline resources using the provided YAML configuration."""
    config = _load_and_validate_config(config_path)
    engine = get_engine(config.engine, config)
    pipeline = Pipeline(config, engine)
    pipeline.create()
    click.echo(f"Pipeline '{config.pipeline_name}' resources created successfully.")


@cli.command()
@click.argument('config_path', type=click.Path(exists=True))
def update(config_path: str):
    """Update pipeline resources using the provided YAML configuration."""
    config = _load_and_validate_config(config_path)
    engine = get_engine(config.engine, config)
    pipeline = Pipeline(config, engine)
    pipeline.update()
    click.echo(f"Pipeline '{config.pipeline_name}' resources updated successfully.")


@cli.command()
@click.argument('config_path', type=click.Path(exists=True))
def test(config_path: str):
    """Test the pipeline using the provided YAML configuration."""
    config = _load_and_validate_config(config_path)
    engine = get_engine(config.engine, config)
    pipeline = Pipeline(config, engine)
    pipeline.test()
    click.echo(f"Pipeline '{config.pipeline_name}' tests executed successfully.")


if __name__ == '__main__':
    cli()

@cli.command()
@click.argument('config_path', type=click.Path(exists=True))
def init(config_path: str):
    """Initialize a new pipeline configuration."""
    config = _load_and_validate_config(config_path)
    engine = get_engine(config.engine, config)
    pipeline = Pipeline(config, engine)
    pipeline.init()
    click.echo(f"Pipeline '{config.pipeline_name}' initialized successfully.")