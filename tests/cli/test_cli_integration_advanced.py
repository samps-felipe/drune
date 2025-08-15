import pytest
import subprocess
import pandas as pd
import os

TEST_CASES = [
    {
        'yml': 'pipeline_complex.yml',
        'csv_out': 'output_complex.csv',
        'expect_success': True,
        'check_cols': ['id', 'codigo', 'valor', 'data_compra', 'descricao', 'ativo']
    },
    {
        'yml': 'pipeline_pk_mista.yml',
        'csv_out': 'output_pk_mista.csv',
        'expect_success': True,
        'check_cols': ['id', 'codigo', 'valor', 'data_compra', 'descricao', 'ativo']
    },
]

BASE_PATH = os.path.join(os.path.dirname(__file__), 'test_files')

@pytest.mark.parametrize('case', TEST_CASES)
def test_cli_advanced(case):
    yml_path = os.path.join(BASE_PATH, case['yml'])
    out_path = os.path.join(BASE_PATH, case['csv_out'])
    # Remove output antes do teste
    if os.path.exists(out_path):
        os.remove(out_path)
    result = subprocess.run([
        'python', '-m', 'declarative_data_framework.cli', 'run', yml_path
    ], capture_output=True, text=True)
    print('CLI stdout:', result.stdout)
    print('CLI stderr:', result.stderr)
    if case['expect_success']:
        assert result.returncode == 0
        assert os.path.exists(out_path)
        df = pd.read_csv(out_path)
        for col in case['check_cols']:
            assert col in df.columns
    else:
        assert result.returncode != 0
