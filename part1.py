from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .appName("MyGoitSparkSandbox-Part1") \
    .getOrCreate()

# Менше шуму в логах
spark.sparkContext.setLogLevel("WARN")

nuek_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv('/opt/bitnami/spark/data/nuek-vuh3.csv')

nuek_repart = nuek_df.repartition(2)

nuek_processed = (nuek_repart
    .where("final_priority < 3")
    .select("unit_id", "final_priority")
    .groupBy("unit_id")
    .count()
)

# Доданий рядок
nuek_processed = nuek_processed.where("count > 2")

# ЄДИНИЙ action тут
nuek_processed.collect()

input("Press Enter to continue...5")

spark.stop()
