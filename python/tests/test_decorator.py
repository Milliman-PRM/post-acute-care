"""
### CODE OWNERS: Ben Copeland, Pierre Cornell

### OBJECTIVE:
  Test the decoration of post-acute care episodes

### DEVELOPER NOTES:
  <none>
"""
# Disable some design quirks required by pytest
# pylint: disable=redefined-outer-name

from pathlib import Path
import datetime

import pytest
from pyspark import Row
import pyspark.sql.functions as spark_funcs

import pac.decorator
from prm.spark.io_txt import build_structtype_from_csv

try:
    _PATH_THIS_FILE = Path(__file__).parent
except NameError:
    _PARTS = list(Path(pac.decorator.__file__).parent.parts)
    _PARTS[-1] = "tests"
    _PATH_THIS_FILE = Path(*_PARTS)  # pylint: disable=redefined-variable-type

PATH_MOCK_SCHEMAS = _PATH_THIS_FILE / "mock_schemas"
PATH_MOCK_DATA = _PATH_THIS_FILE / "mock_data"

# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


@pytest.fixture
def mock_schemas():
    """Schemas for testing data to play with"""
    return {
        path_.stem: build_structtype_from_csv(path_)
        for path_ in PATH_MOCK_SCHEMAS.glob("*.csv")
        }


@pytest.fixture
def mock_dataframes(spark_app, mock_schemas):
    """Testing data to play with"""
    return {
        path_.stem: spark_app.session.read.csv(
            str(path_),
            schema=mock_schemas[path_.stem],
            sep=",",
            header=True,
            mode="FAILFAST",
            )
        for path_ in PATH_MOCK_DATA.glob("*.csv")
        }

def test_flag_index_admissions():
    """Test the flagging of index admissions"""
    mock_member = [
        Row(
            caseadmitid='admit_1',
            fromdate_case=datetime.date(2017, 1, 1),
            episode_start_date=datetime.date(2017, 1, 5),
            episode_end_date=datetime.date(2017, 4, 4)
            ),
        Row(
            caseadmitid='admit_2',
            fromdate_case=datetime.date(2017, 2, 1),
            episode_start_date=datetime.date(2017, 2, 5),
            episode_end_date=datetime.date(2017, 5, 4)
            ),
        Row(
            caseadmitid='admit_3',
            fromdate_case=datetime.date(2017, 5, 1),
            episode_start_date=datetime.date(2017, 5, 5),
            episode_end_date=datetime.date(2017, 8, 4)
            ),
        ]
    expected_index_flags = ['Y', 'N', 'Y']
    result = pac.decorator.flag_index_admissions(mock_member)
    index_flags = [case[-1] for case in result]
    assert index_flags == expected_index_flags


def test_pac(mock_dataframes):
    """Test the avoidable claim identification"""
    test_instance = pac.decorator.PACDecorator()
    actual_result = test_instance.calc_decorator(mock_dataframes)
    actual_result.cache()
    expected_result = mock_dataframes["pac_results_90"].select(
        'sequencenumber',
        *[
            spark_funcs.col(col).alias('expected_' + col)
            for col in mock_dataframes["pac_results_90"].columns
            if col.startswith('pac_')
            ]
    ).cache()
    assert actual_result.count() == expected_result.count()

    compare = expected_result.join(
        actual_result,
        test_instance.key_fields,
        "left_outer",
        ).cache()
    compare_fields = {
        exp_col: exp_col[len("expected_"):]
        for exp_col in expected_result.columns
        if exp_col.startswith("expected_")
        }
    failures = list()
    failure_rows = set()
    for expected_column, actual_column in compare_fields.items():
        misses = compare.filter(compare[expected_column] != compare[actual_column])
        n_misses = misses.count()
        if n_misses != 0:
            failures.append(actual_column)
            failure_rows = failure_rows | set([row['sequencenumber'] for row in misses.collect()])
    assert not failures, "Unexpected values in '{}' column(s) for sequencenumbers '{}'".format(
        failures,
        failure_rows,
        )
