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

#  .option("startingOffsets", "earliest") sert au tout premier démarrage seulement

tickets = raw.select(from_json(col("value").cast("string"), schema).alias("data")) \
             .select("data.*")

tickets = tickets.withColumn(
    "equipe",
    when(col("priorite") == "high", "gestion_crise")
    .when(col("priorite") == "medium", "support_standard")
    .when(col("priorite") == "low", "support_backlog")
)

# Plus d'agrégation ici
tickets_stream = tickets  # ← on passe les tickets bruts au writeStream

def write_batch(df, epoch_id):
    df.cache()  # pour ne pas relire les données depuis redpanda

    # tickets par type_demande et priorité
    df.groupBy("type_demande", "priorite").count() \
      .coalesce(1).write.mode("overwrite").json("/opt/spark/work/output/agg_type_priorite")

    # tickets par équipe
    df.groupBy("equipe").count() \
      .coalesce(1).write.mode("overwrite").json("/opt/spark/work/output/agg_equipe")

    df.unpersist() # enlève le cache
    

query = tickets_stream.writeStream \
    .outputMode("append") \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", "/opt/spark/work/output/checkpoints/ticket_aggregation") \
    .trigger(processingTime="5 seconds") \
    .start()
# trigger périodique avec checkpoint

query.awaitTermination() # à couper manuellemtn