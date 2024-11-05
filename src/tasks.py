import time
from pyspark import SparkContext, SparkConf
from pyspark.conf import SparkConf
from pyspark import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, ArrayType
from pyspark.sql.types import *
from setuptools.command.alias import alias
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


if __name__ == '__main__':
    choose_cat_film_most_money()
