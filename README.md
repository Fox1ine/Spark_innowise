# PySpark Data with Pagila Database

This project demonstrates data analysis using PySpark on tables loaded from the PostgreSQL **Pagila** database. The database is a demo dataset, a variant of the Sakila database for PostgreSQL, which contains information about films, actors, customers, rentals, and more.

## Requirements

- **Apache Spark** (with PySpark)
- **PostgreSQL**
- **Python 3.x**
- Required Python libraries: `pyspark`, `psycopg2`, `pandas` (for PostgreSQL connection and data handling)

## Getting Started

1. **Clone the repository** and set up the environment.
2. **Load Pagila Database**:
   - The Pagila database can be downloaded from [here](https://github.com/devrimgunduz/pagila).
   - Install the database in PostgreSQL by following the instructions in the repository.
3. **Configure Connection**:
   - Set up the PostgreSQL connection details (e.g., `jdbc_url`, `connection_properties`) to access the Pagila database from PySpark.
4. **Load Data into PySpark**:
   - Use PySpark's `read.jdbc()` to load the tables from PostgreSQL into PySpark DataFrames.

## Tasks

The following tasks are performed using PySpark to analyze data from the Pagila database:

1. **Count of Movies by Category**:
   - Output the number of films in each category, sorted in descending order by the count.

2. **Top 10 Most Rented Actors**:
   - Display the top 10 actors whose films were rented the most times, sorted in descending order by the rental count.

3. **Most Expensive Category**:
   - Output the category of films on which the highest amount of money was spent.

4. **Movies Missing in Inventory**:
   - Display the titles of movies that are not present in the `inventory` table.

5. **Top 3 Actors in the "Children" Category**:
   - Output the top 3 actors who have appeared in the most films in the "Children" category. If multiple actors have the same number of films, display all of them.

6. **Cities with Active and Inactive Customers**:
   - Display cities with the number of active and inactive customers (active customers have `customer.active = 1`). Sort the result by the number of inactive customers in descending order.

7. **Categories with Highest Rental Hours in Specific Cities**:
   - Output the category of films with the highest number of total rental hours in cities:
     - Where the city name starts with the letter "A".
     - Where the city name contains the "-" symbol.
   
