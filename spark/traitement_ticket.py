from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StructField, StringType
import psycopg2
import subprocess
import sys

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

# Création de nouvelles colonnes
tickets = tickets.withColumn(
    "equipe",
    when(col("priorite") == "high", "gestion_crise")
    .when(col("priorite") == "medium", "support_standard")
    .when(col("priorite") == "low", "support_backlog")
)

# postgres
POSTGRES_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "tickets_db",
    "user": "root",
    "password": "root",
}

def ensure_tables():
    connection = psycopg2.connect(**POSTGRES_CONFIG)
    connection.autocommit = True
    cursor = connection.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agg_equipe (
            equipe TEXT PRIMARY KEY,
            count BIGINT NOT NULL
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS agg_type_priorite (
            type_demande TEXT NOT NULL,
            priorite TEXT NOT NULL,
            count BIGINT NOT NULL,
            PRIMARY KEY (type_demande, priorite)
        )
    """)

    cursor.close()
    connection.close()

ensure_tables()


# agrégation et écriture dbb
def write_batch(df, epoch_id):
    equipe_counts = df.groupBy("equipe").count().collect()
    type_priorite_counts = df.groupBy("type_demande", "priorite").count().collect()

    connection = psycopg2.connect(**POSTGRES_CONFIG)
    cursor = connection.cursor()

    for row in equipe_counts:
        cursor.execute("""
            INSERT INTO agg_equipe (equipe, count)
            VALUES (%s, %s)
            ON CONFLICT (equipe)
            DO UPDATE SET count = agg_equipe.count + EXCLUDED.count
        """, (row["equipe"], row["count"]))

    for row in type_priorite_counts:
        cursor.execute("""
            INSERT INTO agg_type_priorite (type_demande, priorite, count)
            VALUES (%s, %s, %s)
            ON CONFLICT (type_demande, priorite)
            DO UPDATE SET count = agg_type_priorite.count + EXCLUDED.count
        """, (row["type_demande"], row["priorite"], row["count"]))

    connection.commit()
    cursor.close()
    connection.close()
    

query = tickets.writeStream \
    .outputMode("append") \
    .foreachBatch(write_batch) \
    .option("checkpointLocation", "/opt/spark/work/output/checkpoints/ticket_aggregation") \
    .trigger(processingTime="5 seconds") \
    .start()
# trigger périodique avec checkpoint
# checkpoint mémorise le dernier offset validé

try:
    query.awaitTermination() # à couper manuellemtn
finally:
    subprocess.run(
        [sys.executable, "/opt/spark/work/export_final_json.py"],
        check=True
    )