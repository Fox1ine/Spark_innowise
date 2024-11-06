import time
from pyspark import SparkContext, SparkConf
from pyspark.conf import SparkConf
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.types import *
from setuptools.command.alias import alias
from unicodedata import category
from pyspark.sql.functions import col, sum as spark_sum, desc, unix_timestamp, expr
from config.config import *
from pyspark.sql import functions as f
from src.dataframes import *


# region Task 1
# -- Output the count of films in each category, sorted in descending order.--
def sum_film_in_category():
    df_joined = df_category.join(df_film_category, df_category.category_id == df_film_category.category_id, how='inner') \
        .join(df_film, df_film.film_id == df_film_category.film_id, how='inner')

    select_count_of_films = df_joined.groupBy(df_category["name"].alias("category_name")) \
        .count() \
        .orderBy(desc("count"))

    select_count_of_films.show()


# endregion

# region Task 2
# --Output the 10 actors whose films have rented the most, sorted in descending order.--


def choose_top_act_acc_rent():
    df_joined = df_rental \
        .join(df_inventory, df_rental.inventory_id == df_inventory.inventory_id, how='inner') \
        .join(df_film_actor, df_inventory.film_id == df_film_actor.film_id, how='inner') \
        .join(df_actor, df_actor.actor_id == df_film_actor.actor_id, how='inner')

    top_ten = df_joined.groupBy(df_actor["first_name"], df_actor["last_name"]) \
        .agg(count("*").alias("count_of_rentals")) \
        .orderBy(desc("count_of_rentals")) \
        .limit(10)

    top_ten.select("first_name", "last_name", "count_of_rentals").show()


# endregion

# region Task 3
# --Output the category of films on which the most money was spent.--

def choose_cat_film_most_money():
    df_joined = df_category \
        .join(df_film_category, df_category.category_id == df_film_category.category_id, how='inner') \
        .join(df_inventory, df_inventory.film_id == df_film_category.film_id, how='inner') \
        .join(df_rental, df_inventory.inventory_id == df_rental.inventory_id, how='inner') \
        .join(df_payment, df_rental.rental_id == df_payment.rental_id, how='inner')

    max_money_spend = df_joined \
        .groupBy(df_category["name"].alias("Name of category")) \
        .agg(sum(df_payment['amount']).alias("total_spent")) \
        .orderBy(desc("total_spent")) \
        .limit(1)

    max_money_spend.show()


# endregion

# region Task 4
# --Output the titles of films that are not in the inventory.--

def choose_not_in_film():
    df_joined = df_film.join(df_inventory, df_film.film_id == df_inventory.film_id, how='left_anti').select(
        df_film['title'])

    df_joined.show()


# endregion

# region Task 5
# --Output the top 3 actors who have appeared in the most films in the ‘Children’ category.
# If several actors have the same number of films, output all...--

def choose_top_actors():
    df_joined = df_actor.join(df_film_actor, df_actor.actor_id == df_film_actor.actor_id, how='inner')\
                        .join(df_film_category, df_film_actor.film_id == df_film_category.film_id, how='inner')\
                        .join(df_category, df_film_category.category_id == df_category.category_id, how='inner')


    df_filtred = df_joined.filter(df_category['name'] == 'Children') \
                            .groupBy(df_actor['first_name'])\
                            .agg(count("*").alias("count_of_actors"))\
                            .orderBy(desc("count_of_actors"))

    top_act = df_filtred.limit(3)
    top_act.show()
# endregion

# region Task 6
# --Output cities with the number of active and inactive customers (active - customer.active = 1).
# Sort by the number of inactive customers in descending order.--

def choose_cities():
    df_joined = df_address.join(df_city,df_address.city_id == df_city.city_id, how='inner')\
                            .join(df_customer, df_address.address_id == df_customer.address_id, how='inner')

    df_filtred_data = df_joined.groupBy(df_city['city'])\
                                .agg(
                                    sum((df_customer['active'] == 1).cast('int')).alias('active_customers'),
                                    sum((df_city['city'] == "0").cast('int')).alias('inactive_customers'),
                                    )\
                                .orderBy(desc("inactive_customers"))

    active_customers_df = df_filtred_data.select("city", "active_customers")\
                                    .filter("active_customers > 0")\
                                    .orderBy(desc("active_customers"))

    inactive_customers_df = df_filtred_data.select("city", "inactive_customers")\
                                      .filter("inactive_customers == 0")\
                                      .orderBy(desc("inactive_customers"))


    active_customers_df.show()
    inactive_customers_df.show()

# endregion

# region Task 7
# Output the category of films that have the highest number of total rental hours in cities (customer.address_id in this city),
# and that start with the letter ‘a’. Do the same for cities with a ‘-’ symbol.--

def choose_category_hours_in_cities():
    df_joined = df_category \
        .join(df_film_category, df_category.category_id == df_film_category.category_id, how='inner') \
        .join(df_inventory, df_film_category.film_id == df_inventory.film_id, how='inner') \
        .join(df_rental, df_inventory.inventory_id == df_rental.inventory_id, how='inner') \
        .join(df_customer, df_rental.customer_id == df_customer.customer_id, how='inner') \
        .join(df_address, df_customer.address_id == df_address.address_id, how='inner') \
        .join(df_city, df_address.city_id == df_city.city_id, how='inner')

    df_filtred = df_joined\
        .filter((col('city').startswith('a')) | (col('city').contains('-')))

    df_with_time = df_filtred\
        .withColumn('rental_hours',
        (unix_timestamp(col('return_date')) - unix_timestamp(col('rental_date'))) / 3600
    )

    category_hours = df_with_time\
        .groupBy('name') \
        .agg(spark_sum("rental_hours").alias("total_rental_hours")) \
        .orderBy(desc("total_rental_hours"))


    category_hours.show()

# endregion


if __name__ == '__main__':
    choose_category_hours_in_cities()
