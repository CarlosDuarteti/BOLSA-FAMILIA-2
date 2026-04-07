from pyspark.sql import SparkSession

def ler_dados(caminho_csv):
    spark = SparkSession.builder\
    .appName("Bolsa Familia")\
    .config("spark.driver.memory", "8g")\
    .config("spark.executor.memory", "8g")\
    .config("spark.sql.shuffle.partitions", "50")\
    .getOrCreate()

    df = spark.read\
    .option("header", True)\
    .option("InferSchema", True)\
    .option("sep", ";")\
    .option("encoding","ISO-8859-1")\
    .csv(caminho_csv)

    return df