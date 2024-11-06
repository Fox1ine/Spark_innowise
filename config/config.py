from pyspark.sql import SparkSession

jdbc_driver_path = r"D:\ProjectsPy\spark_t\jars\postgresql-42.7.4.jar"


spark = SparkSession.builder \
    .appName("PostgreSQL_Spark") \
    .master("local[*]") \
    .config("spark.driver.extraClassPath", jdbc_driver_path) \
    .getOrCreate()


jdbc_url = "jdbc:postgresql://localhost:5432/pagila"


connection_properties = {
    "user": "postgres",
    "password": "123",
    "driver": "org.postgresql.Driver"
}