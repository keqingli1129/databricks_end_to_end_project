from pyspark import pipelines as dp
from pyspark.sql import functions as F

SOURCE = f"{spark.conf.get('cdc.source_schema')}.claim"
CHANGE_FEED_COLUMNS = ["_change_type", "_commit_version", "_commit_timestamp"]


@dp.temporary_view()
def claim_changes():
    """Every insert/update/delete on the source table; updates keep only the new values."""
    return (
        spark.readStream.option("readChangeFeed", "true")
        .table(SOURCE)
        .filter("_change_type != 'update_preimage'")
    )


dp.create_streaming_table(name="claim", comment="Claims from the source database, kept in sync by AUTO CDC.")

dp.create_auto_cdc_flow(
    target="claim",
    source="claim_changes",
    keys=["claim_no"],
    sequence_by="_commit_version",
    apply_as_deletes=F.expr("_change_type = 'delete'"),
    except_column_list=CHANGE_FEED_COLUMNS,
    stored_as_scd_type=1,
)
