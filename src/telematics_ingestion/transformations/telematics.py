from pyspark import pipelines as dp

from databricks_end_to_end_project.telematics.parsing import parse_telematics


@dp.table(comment="Parsed telematics events: one column per field, values as strings.")
def telematics():
    return parse_telematics(spark.readStream.table("telematics_raw"))
