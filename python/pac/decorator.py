"""
### CODE OWNERS: Ben Copeland

### OBJECTIVE:
  Maintain the logic for calculating post-acute care decorators

### DEVELOPER NOTES:
  Will share some tooling with analytics-pipeline library
"""
import logging

from pyspark.sql import DataFrame, Window
import pyspark.sql.functions as spark_funcs
import pyspark.sql.types as spark_types

from prm.decorators.base_classes import ClaimDecorator

LOGGER = logging.getLogger(__name__)

_IP_MR_LINES = {
    'I11',
    'I12',
    }

# pylint: disable=no-member

# =============================================================================
# LIBRARIES, LOCATIONS, LITERALS, ETC. GO ABOVE HERE
# =============================================================================


def flag_index_admissions(
        episodes: "typing.Iterable[typing.Tuple[str, datetime.date, datetime.date, datetime.date]]",
    ) -> "typing.Iterable[typing.Tuple[str, datetime.date, datetime.date, str]]":
    """Determine if an admission should be considered an index admission"""
    admits_sorted = sorted(
        episodes,
        key=lambda row: (
            int(row["episode_start_date"].toordinal()),
            int(row["fromdate_case"].toordinal()),
            row['caseadmitid'],
            ),
        )
    admits_decorated = list()
    last_episode_end_date = None
    for admit in admits_sorted:
        if (
                not last_episode_end_date
                or admit['fromdate_case'] > last_episode_end_date
            ):
            index_yn = 'Y'
            last_episode_end_date = admit['episode_end_date']
        else:
            index_yn = 'N'
        admits_decorated.append(
            (admit['caseadmitid'], admit['episode_start_date'], admit['episode_end_date'], index_yn)
            )
    return admits_decorated


def _categorize_claims(
        outclaims: DataFrame,
    ) -> DataFrame:
    """Flag service categories that are relevant to PAC"""
    claims_categorized = outclaims.select(
        '*',
        spark_funcs.when(
            spark_funcs.substring(
                spark_funcs.col('mr_line'),
                1,
                3,
            ).isin(_IP_MR_LINES),
            spark_funcs.lit('IP')
        ).when(
            spark_funcs.col('mr_line') == 'I31',
            spark_funcs.lit('SNF'),
        ).when(
            spark_funcs.col('mr_line') == 'P82a',
            spark_funcs.lit('HH'),
        ).otherwise('Other').alias('pac_major_category'),
        spark_funcs.substring(
            spark_funcs.col('prv_id_ccn'),
            3,
            4,
        ).cast('int').alias('_ccn_last_four'),
        spark_funcs.substring(
            spark_funcs.col('prv_id_ccn'),
            3,
            1,
        ).alias('_ccn_third_char'),
    ).withColumn(
        'pac_minor_category',
        spark_funcs.when(
            spark_funcs.col('pac_major_category') == 'IP',
            spark_funcs.when(
                spark_funcs.col('drg').isin('945', '946'),
                spark_funcs.lit('Rehab'),
            ).when(
                (spark_funcs.col('_ccn_last_four') >= 1)
                & (spark_funcs.col('_ccn_last_four') <= 879),
                spark_funcs.lit('Acute'),
            ).when(
                (spark_funcs.col('_ccn_last_four') >= 1300)
                & (spark_funcs.col('_ccn_last_four') <= 1399),
                spark_funcs.lit('Acute'),
            ).when(
                (spark_funcs.col('_ccn_last_four') >= 2000)
                & (spark_funcs.col('_ccn_last_four') <= 2299),
                spark_funcs.lit('Rehab'),
            ).when(
                (spark_funcs.col('_ccn_last_four') >= 3025)
                & (spark_funcs.col('_ccn_last_four') <= 3099),
                spark_funcs.lit('Rehab'),
            ).when(
                spark_funcs.col('_ccn_third_char').isin('T', 'R'),
                spark_funcs.lit('Rehab'),
            ).when(
                spark_funcs.col('mr_line').isin('I11b', 'I11c'),
                spark_funcs.lit('Rehab'),
            ).when(
                spark_funcs.col('mr_line').isin('I11a', 'I12'),
                spark_funcs.lit('Acute'),
            ).otherwise(
                spark_funcs.lit('Other'),
            )
        ).otherwise('Other'),
    )
    return claims_categorized


def _collect_pac_eligible_ip_stays(
        claims_categorized: DataFrame,
        episode_length: int,
    ) -> DataFrame:
    """Find all IP stays that could initiate a PAC episode"""
    ip_claims = claims_categorized.filter(
        (spark_funcs.col('pac_major_category') == 'IP')
        & (spark_funcs.col('pac_minor_category') == 'Acute')
    ).groupBy(
        'member_id',
        'caseadmitid',
    ).agg(
        spark_funcs.min('admitdate').alias('fromdate_case'),
        spark_funcs.max('dischdate').alias('todate_case'),
    ).select(
        '*',
        spark_funcs.col('todate_case').alias('episode_start_date'),
        spark_funcs.date_add(
            spark_funcs.col('todate_case'),
            episode_length,
            ).alias('episode_end_date'),
    )

    transfer_window = Window().partitionBy(
        'member_id',
    ).orderBy(
        'fromdate_case',
        'todate_case',
        'episode_start_date',
        'episode_end_date',
    )

    ip_transfers = ip_claims.withColumn(
        'next_fromdate_case',
        spark_funcs.lead('fromdate_case').over(transfer_window)
    ).withColumn(
        'transfer_yn',
        spark_funcs.when(
            spark_funcs.date_add(
                'todate_case',
                1
                ) >= spark_funcs.col('next_fromdate_case'),
            'Y',
        ).otherwise('N')
    ).filter(
        'transfer_yn = "N"'
    )

    ip_episode_struct = ip_transfers.select(
        'member_id',
        spark_funcs.struct(
            'caseadmitid',
            'fromdate_case',
            'episode_start_date',
            'episode_end_date',
            ).alias('struct_admits')
    ).groupBy(
        'member_id'
    ).agg(
        spark_funcs.collect_list(spark_funcs.col('struct_admits')).alias('array_admits')
    )
    return ip_episode_struct


def _find_index_admissions(
        pac_eligible_ip: DataFrame,
    ) -> DataFrame:
    """Find IP cases that have a valid post-acute care episode"""
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
    ip_index_episodes = pac_eligible_ip.select(
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
    return ip_index_episodes


def _decorate_claims_detail(
        claims_categorized,
        ip_index_episodes,
    ) -> DataFrame:
    """Merge episode information back onto claims detail"""
    claims_w_indexes = claims_categorized.join(
        ip_index_episodes.drop('pac_index_yn'),
        on=(
            (claims_categorized['member_id'] == ip_index_episodes['member_id'])
            & (claims_categorized['fromdate'] > ip_index_episodes['pac_episode_start_date'])
            & (claims_categorized['fromdate'] < ip_index_episodes['pac_episode_end_date'])
            ) | (
                (claims_categorized['member_id'] == ip_index_episodes['member_id'])
                & (claims_categorized['caseadmitid'] == ip_index_episodes['pac_caseadmitid'])
            ),
        how='left_outer',
    ).select(
        '*',
        spark_funcs.greatest(
            spark_funcs.datediff(
                spark_funcs.col('fromdate'),
                spark_funcs.col('pac_episode_start_date'),
                ),
            spark_funcs.lit(0),
            ).alias('pac_days_since_episode_start'),
        spark_funcs.when(
            spark_funcs.col('pac_caseadmitid').isNotNull(),
            'Y'
            ).otherwise('N').alias('pac_claim_yn'),
        spark_funcs.when(
            spark_funcs.col('pac_caseadmitid') == spark_funcs.col('caseadmitid'),
            spark_funcs.lit('Y'),
            ).otherwise('N').alias('pac_index_yn'),
    )
    claims_decoratored = claims_w_indexes.select(
        'sequencenumber',
        *[
            column for column in claims_w_indexes.columns
            if column.startswith('pac_')
            ],
    )
    return claims_decoratored


def calculate_post_acute_episodes(
        dfs_input: "typing.Mapping[str, DataFrame]",
        *,
        episode_length: int=90
    ) -> DataFrame:
    """Define the post-acute care episodes"""
    LOGGER.info('Calculating post-acute care decorators')
    claims_categorized = _categorize_claims(
        dfs_input['outclaims']
        )

    pac_eligible_ip = _collect_pac_eligible_ip_stays(
        claims_categorized,
        episode_length,
        )

    ip_index_episodes = _find_index_admissions(
        pac_eligible_ip,
        )

    claims_decorated = _decorate_claims_detail(
        claims_categorized,
        ip_index_episodes,
        )
    return claims_decorated


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
