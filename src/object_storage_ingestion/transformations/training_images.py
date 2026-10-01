from pyspark import pipelines as dp

SOURCE_PATH = spark.conf.get("object_storage.training_images_path")


@dp.table(
    comment="Car-damage training images (ok / minor / major in the file name), ingested as binary files.",
    table_properties={"quality": "bronze"},
)
def training_images():
    return spark.readStream.format("cloudFiles").option("cloudFiles.format", "binaryFile").load(SOURCE_PATH)
