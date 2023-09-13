# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import  rank, when, row_number, col, desc, year, sum
from pyspark.sql.window import Window


# %%
spark = SparkSession.builder.appName('Individual_Spark')\
        .config('spark.driver.extraClassPath','/Users/anujkhadka/Fusemachines47/AllSPARK/Apache-Spark/spark-3.4.1-bin-hadoop3/jars/postgresql-42.6.0.jar')\
        .getOrCreate()

# %%

# %% [markdown]
# Loading the datasets

# %%
revised_final = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "revised_final") \
    .option("user", "ujjwal") \
    .option("password", "ujjwal12345") \
    .option("driver", "org.postgresql.Driver") \
    .load()


# %%
goods_final = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "goods_final") \
    .option("user", "ujjwal") \
    .option("password", "ujjwal12345") \
    .option("driver", "org.postgresql.Driver") \
    .load()


# %%
country_final = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "country_final") \
    .option("user", "ujjwal") \
    .option("password", "ujjwal12345") \
    .option("driver", "org.postgresql.Driver") \
    .load()


# %%
services_final= spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://localhost:5432/postgres") \
    .option("dbtable", "services_final") \
    .option("user", "ujjwal") \
    .option("password", "ujjwal12345") \
    .option("driver", "org.postgresql.Driver") \
    .load()

# %% [markdown]
# Q.1 Compute the following metrics for each country:
# 
# The sum of total export values.
# 
# The sum of total import values.
# 
# The difference between the sum of export and import values as ‘Trade balance’.
# 
# Now, Join  'revised_final" table with the 'country_final' table  & filter the result to select the peak countries with the highest trade balance.

# %%
filtered_df = revised_final.filter((year(col("time_ref")) == 2023) & (col("account").isin("Exports", "Imports")))

trade_balance_df = filtered_df.groupBy("country_code").agg(
    sum(
        when(col("account") == "Exports", col("value"))
        .otherwise(-col("value"))
    ).alias("trade_balance"),
    sum(
        when(col("account") == "Exports", col("value"))
        .otherwise(0)
    ).alias("export_value"),
    sum(
        when(col("account") == "Imports", col("value"))
        .otherwise(0)
    ).alias("import_value")
)

window_spec = Window.orderBy(desc("trade_balance"))

ranked_df = trade_balance_df.withColumn("rank", row_number().over(window_spec))

top_countries_df = ranked_df.filter(col("rank") <= 20)

result_df = top_countries_df.join(
    country_final,
    top_countries_df["country_code"] == country_final["country_code"],
    "inner"
).select(
    "country_label", 
    "rank",
    "export_value",
    "import_value",
    "trade_balance"
)

result_df.show(truncate = False)


# %% [markdown]
# Q.2 Show the comparative study of the average transactions of 'account' column (export + import) for different product types (Goods & Services) from 2020 to 2023 combined in a table and show it in columns using Pivot DataFrame Transformations.

# %%
filtered_df = revised_final.filter(
    (year(col("time_ref")) >= 2020) & (col("account").isin("Exports", "Imports")) & (col("product_type") == "Goods")
)

filtered_df = filtered_df.withColumn("year", year("time_ref"))

avg_exports_df = filtered_df.groupBy("country_code").pivot("year").agg({"value": "avg"})

result_df = avg_exports_df.join(
    country_final,
    avg_exports_df["country_code"] == country_final["country_code"],
    "left"
).select(
    "country_label",
    "2020",
    "2021",
    "2022",
    "2023"
)

result_df.show(truncate = False)

# %%
filtered_df = revised_final.filter(
    (year(col("time_ref")) >= 2020) & (col("account").isin("Exports", "Imports")) & (col("product_type") == "Services")
)

filtered_df = filtered_df.withColumn("year", year("time_ref"))

avg_exports_df = filtered_df.groupBy("country_code").pivot("year").agg({"value": "avg"})

result_df = avg_exports_df.join(
    country_final,
    avg_exports_df["country_code"] == country_final["country_code"],
    "left"
).select(
    "country_label",
    "2020",
    "2021",
    "2022",
    "2023"
)

result_df.show(truncate = False)




# %%



