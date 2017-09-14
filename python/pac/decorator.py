"""
### CODE OWNERS: Ben Copeland

### OBJECTIVE:
  Maintain the logic for calculating post-acute care decorators

### DEVELOPER NOTES:
  Will share some tooling with analytics-pipeline library
"""
import logging
import typing

from pyspark.sql import DataFrame
import pyspark.sql.functions as spark_funcs

from prm.decorators.base_classes import ClaimDecorator

LOGGER = logging.getLogger(__name__)

_ACUTE_MR_LINES = {
    'I11',
    'I12',
    }

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def calculate_post_acute_episodes(
    dfs_input: typing.Mapping[str, DataFrame],
    *,
    episode_length: int=90
    ) -> DataFrame:
    """Define the post-acute care episodes"""
    ip_claims = dfs_input['outclaims'].filter(
        spark_funcs.substring(
            spark_funcs.col('mr_line'),
            1,
            3,
        ).isin(_ACUTE_MR_LINES)
    ).select(
        '*',
        spark_funcs.col('dischdate').alias('episode_start_date'),
        spark_funcs.date_add(
            spark_funcs.col('dischdate'),
            episode_length,
            ).alias('episode_end_date'),
    )


class PACDecorator(ClaimDecorator):
    """Calculate the post-acute care decorators"""
    @staticmethod
    def validate_decor_column_name(name: str) -> bool:
        """Defines what naming convention the decorator columns should follow"""
        return name.startswith("pac_")

    def _calc_decorator(
            self,
            dfs_input: typing.Mapping[str, DataFrame],
        ) -> DataFrame: