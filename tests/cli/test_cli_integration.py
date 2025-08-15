# Teste de integração real: cria arquivos .yml e .csv, executa CLI e valida saída
import pytest
import yaml
import pandas as pd
import os
import subprocess

def test_cli_pipeline_real(tmp_path):
    # 1. Cria input.csv real
    df = pd.DataFrame({'col1': [1, 2, 3], 'col2': ['a', 'b', 'c']})
    input_csv = tmp_path / 'input.csv'
    df.to_csv(input_csv, index=False)

    # 2. Cria pipeline.yml real
    output_csv = tmp_path / 'output.csv'
    pipeline_config = {
        'pipeline_name': 'test_pipeline',
        'engine': 'pandas',
        'pipeline_type': 'silver',
        'source': {'format': 'csv', 'path': str(input_csv)},
        'sink': {'format': 'csv', 'mode': 'overwrite', 'path': str(output_csv)},
        'columns': [{'name': 'col1', 'type': 'int'}, {'name': 'col2', 'type': 'string'}],
        'steps': [
            {'name': 'read', 'type': 'read', 'params': {}},
            {'name': 'write', 'type': 'write', 'params': {}}
        ]
    }
    pipeline_yml = tmp_path / 'pipeline.yml'
    with open(pipeline_yml, 'w') as f:
        yaml.dump(pipeline_config, f)

    # 3. Executa CLI real via subprocess
    result = subprocess.run([
        'python', '-m', 'declarative_data_framework.cli', 'run', str(pipeline_yml)
    ], capture_output=True, text=True)

    print('CLI stdout:', result.stdout)
    print('CLI stderr:', result.stderr)
    assert result.returncode == 0
    # 4. Valida arquivo de saída
    assert output_csv.exists()
    df_out = pd.read_csv(output_csv)
    assert 'col1' in df_out.columns and 'col2' in df_out.columns

# Outros testes podem ser criados para pipelines inválidos, formatos diferentes, etc.
