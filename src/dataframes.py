from pyspark import SparkContext, SparkConf
from pyspark.conf import SparkConf
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.types import *
from  config.config import *



df_actor = spark.read.jdbc(url=jdbc_url,
                           table='actor',
                           properties=connection_properties
                           ).alias('actor')

df_address = spark.read.jdbc(url=jdbc_url,
                             table='address',
                             properties=connection_properties
                             ).alias('address')

df_category = spark.read.jdbc(url=jdbc_url,
                              table='category',
                              properties=connection_properties
                              ).alias('category')

df_city = spark.read.jdbc(url=jdbc_url,
                          table='city',
                          properties=connection_properties
                          ).alias('city')

df_country = spark.read.jdbc(url=jdbc_url,
                             table='country',
                             properties=connection_properties
                             ).alias('country')

df_customer = spark.read.jdbc(url=jdbc_url,
                              table='customer',
                              properties=connection_properties
                              ).alias('customer')

df_film = spark.read.jdbc(url=jdbc_url,
                          table='film',
                          properties=connection_properties
                          ).alias('film')

df_film_actor = spark.read.jdbc(url=jdbc_url,
                                table='film_actor',
                                properties=connection_properties
                                ).alias('film_actor')

df_film_category = spark.read.jdbc(url=jdbc_url,
                                   table='film_category',
                                   properties=connection_properties
                                   ).alias('film_category')

df_inventory = spark.read.jdbc(url=jdbc_url,
                               table='inventory',
                               properties=connection_properties
                               ).alias('inventory')

df_language = spark.read.jdbc(url=jdbc_url,
                              table='language',
                              properties=connection_properties
                              ).alias('language')

df_payment = spark.read.jdbc(url=jdbc_url,
                             table='payment',
                             properties=connection_properties
                             ).alias('payment')

df_rental = spark.read.jdbc(url=jdbc_url,
                            table='rental',
                            properties=connection_properties
                            ).alias('rental')

df_staff = spark.read.jdbc(url=jdbc_url,
                           table='staff',
                            properties=connection_properties
                           ).alias('staff')

df_store = spark.read.jdbc(url=jdbc_url,
                           table='store',
                           properties=connection_properties
                           ).alias('store')
