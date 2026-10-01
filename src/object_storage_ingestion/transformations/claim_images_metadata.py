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
        # Default mode, written out to make it visible: new columns are added to the table schema.
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .load(SOURCE_PATH)
    )
