"""
### CODE OWNERS: Ben Copeland

### OBJECTIVE:
  Maintain the logic for calculating post-acute care decorators

### DEVELOPER NOTES:
  Will share some tooling with analytics-pipeline library
"""
import logging
import typing
import datetime

from pyspark.sql import DataFrame
import pyspark.sql.functions as spark_funcs
import pyspark.sql.types as spark_types

from prm.decorators.base_classes import ClaimDecorator

LOGGER = logging.getLogger(__name__)

_ACUTE_MR_LINES = {
    'I11',
    'I12',
    }

# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def flag_index_admissions(
        episodes: "typing.Iterable[typing.Tuple[datetime.date, datetime.date]]",
    ) -> "typing.Iterable[typing.Tuple[datetime.date, datetime.date, str]]":
    """Determine if an admission should be considered an index admission"""
    admits_sorted = sorted(
        episodes,
        key=lambda row: (
            int(row["episode_start_date"].toordinal()),
            -int(row["episode_end_date"].toordinal()),
            row['caseadmitid'],
            ),
        )
    admits_decorated = list()
    last_episode_end_date = None
    for admit in admits_sorted:
        if (
                not last_episode_end_date
                or admit['episode_start_date'] > last_episode_end_date
            ):
            index_yn = 'Y'
            last_episode_end_date = admit['episode_end_date']
        else:
            index_yn = 'N'
        admits_decorated.append(
            (admit['caseadmitid'], admit['episode_start_date'], admit['episode_end_date'], index_yn)
            )
    return admits_decorated


def calculate_post_acute_episodes(
        dfs_input: "typing.Mapping[str, DataFrame]",
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
        'member_id',
        'caseadmitid',
        spark_funcs.col('dischdate').alias('episode_start_date'),
        spark_funcs.date_add(
            spark_funcs.col('dischdate'),
            episode_length,
            ).alias('episode_end_date'),
    ).distinct()

    ip_episode_struct = ip_claims.select(
        'member_id',
        spark_funcs.struct(
            'caseadmitid',
            'episode_start_date',
            'episode_end_date',
            ).alias('struct_admits')
    ).groupBy(
        'member_id'
    ).agg(
        spark_funcs.collect_list(spark_funcs.col('struct_admits')).alias('array_admits')
    )

    struct_expected = spark_types.ArrayType(
        spark_types.StructType([
            spark_types.StructField('caseadmitid', spark_types.StringType()),
            spark_types.StructField('episode_start_date', spark_types.DateType()),
            spark_types.StructField('episode_end_date', spark_types.DateType()),
            spark_types.StructField('index_yn', spark_types.StringType()),
            ])
        )

    index_udf = spark_funcs.udf(
        flag_index_admissions,
        struct_expected,
        )
    ip_index_episodes = ip_episode_struct.select(
        'member_id',
        spark_funcs.explode(
            index_udf(spark_funcs.col('array_admits'))
        ).alias('struct_indexes')
    ).select(
        'member_id',
        spark_funcs.col('struct_indexes')['caseadmitid'].alias('pac_caseadmitid'),
        spark_funcs.col('struct_indexes')['episode_start_date'].alias('pac_episode_start_date'),
        spark_funcs.col('struct_indexes')['episode_end_date'].alias('pac_episode_end_date'),
        spark_funcs.col('struct_indexes')['index_yn'].alias('pac_index_yn'),
    ).filter(
        'pac_index_yn = "Y"'
    )

    ip_index_episodes.select(
        'member_id',
        spark_funcs.col('pac_episode_start_date').alias('date_start'),
        spark_funcs.col('pac_episode_end_date').alias('date_end'),
    ).validate.assert_window_format(
        'member_id',
        tolerance=1.0,
    )

    claims_w_indexes = dfs_input['outclaims'].join(
        ip_index_episodes,
        on=(
            (dfs_input['outclaims']['member_id'] == ip_index_episodes['member_id'])
            & (dfs_input['outclaims']['fromdate'] > ip_index_episodes['pac_episode_start_date'])
            & (dfs_input['outclaims']['fromdate'] < ip_index_episodes['pac_episode_end_date'])
            ),
        how='left_outer',
    ).select(
        '*',
        spark_funcs.datediff(
            spark_funcs.col('fromdate'),
            spark_funcs.col('pac_episode_start_date'),
            ).alias('pac_days_since_episode_start'),
        spark_funcs.when(
            spark_funcs.col('pac_caseadmitid').isNotNull(),
            'Y'
            ).otherwise('N').alias('pac_claim_yn'),
    )
    return claims_w_indexes.select(
        'sequencenumber',
        *[
            column for column in claims_w_indexes.columns
            if column.startswith('pac_')
            ],
    )


class PACDecorator(ClaimDecorator):
    """Calculate the post-acute care decorators"""
    @staticmethod
    def validate_decor_column_name(name: str) -> bool:
        """Defines what naming convention the decorator columns should follow"""
        return name.startswith("pac_")

    def _calc_decorator(
            self,
            dfs_input: "typing.Mapping[str, DataFrame]",
        ) -> DataFrame:
        return calculate_post_acute_episodes(
            dfs_input,
            )
