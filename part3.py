from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .appName("MyGoitSparkSandbox-Part3") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

nuek_df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv('/opt/bitnami/spark/data/nuek-vuh3.csv')

nuek_repart = nuek_df.repartition(2)

nuek_processed_cached = (nuek_repart
    .where("final_priority < 3")
    .select("unit_id", "final_priority")
    .groupBy("unit_id")
    .count()
    .cache()   # ключова відмінність
)

# ACTION #1: матеріалізує кеш
nuek_processed_cached.collect()

# Далі працюємо з кешованим
nuek_processed = nuek_processed_cached.where("count > 2")

# ACTION #2
nuek_processed.collect()

input("Press Enter to continue...5")

# Звільнити кеш (добра практика)
nuek_processed_cached.unpersist()

spark.stop()
