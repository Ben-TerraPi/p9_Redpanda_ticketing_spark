from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType

schema = StructType([
    StructField("id_ticket", StringType()),
    StructField("id_client", StringType()),
    StructField("date_time", StringType()),
    StructField("demande", StringType()),
    StructField("type_demande", StringType()),
    StructField("priorite", StringType())
])

spark = SparkSession.builder \
    .appName("TicketAggregation") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "redpanda-0:9092") \
    .option("subscribe", "client_tickets") \
    .option("startingOffsets", "earliest") \
    .load()

tickets = raw.select(from_json(col("value").cast("string"), schema).alias("data")) \
             .select("data.*")

tickets = tickets.withColumn(
    "equipe",
    when(col("priorite") == "high", "gestion_crise")
    .when(col("priorite") == "medium", "support_standard")
    .when(col("priorite") == "low", "support_backlog")
)

aggregation = tickets.groupBy("type_demande", "priorite").count()

def write_batch(df, epoch_id):
    df.write.mode("overwrite").json("/tmp/output/tickets_aggregation")

query = aggregation.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_batch) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()