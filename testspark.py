from databricks.connect import DatabricksSession
spark = DatabricksSession.builder.profile("DEFAULT").serverless().getOrCreate()
spark.read.table("samples.nyctaxi.trips").limit(5).show()
