from pyspark import pipelines as dp

SOURCE_PATH = spark.conf.get("object_storage.claim_metadata_path")


@dp.table(
    comment="Metadata of customer-uploaded claim images: which image belongs to which claim and car.",
    table_properties={"quality": "bronze"},
)
def claim_images_metadata():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        # Rescue mode: the table schema stays fixed; values of unknown columns go into _rescued_data.
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .load(SOURCE_PATH)
    )
