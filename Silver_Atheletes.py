# Databricks notebook source
# MAGIC %md
# MAGIC ### Reading the Athletes Data

# COMMAND ----------

df_ath = spark.read.format("parquet")\
                .load("abfss://bronze@projcicddevaish242.dfs.core.windows.net/athletes")

# COMMAND ----------

display(df_ath)

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filling the NULL 

# COMMAND ----------

df_ath =df_ath.fillna({"birth_place":"xyz","birth_country":"abc","residence_place":"fgq","residence_country":"efg"})

# COMMAND ----------

display(df_ath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Filtering the data

# COMMAND ----------

df_fi = df_ath.filter((col("current")==True)&(col('name').isin('ALEKSANYAN Artur','GALSTYAN Slavik','TEVANYAN Vazgen')))

# COMMAND ----------

display(df_fi)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Casting the data type

# COMMAND ----------

df_ath = df_ath.withColumn('height',col('height').cast(FloatType()))\
               .withColumn('weight',col('weight').cast(FloatType()))

# COMMAND ----------

display(df_ath)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sorting the height & weight data

# COMMAND ----------

df_sorted = df_ath.sort('height','weight',ascending=[0,1])

# COMMAND ----------

display(df_sorted)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Using regexp_replace 

# COMMAND ----------

df_sorted = df_sorted.withColumn('nationality',regexp_replace('nationality','United States','US'))

# COMMAND ----------

display(df_sorted)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Groupby code with count

# COMMAND ----------

df_ath.groupBy('code').agg(count('code')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### applying the agg on group by data

# COMMAND ----------

df_ath.groupBy('code').agg(count('code').alias('total_count')).filter(col('total_count')>1).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### column rename

# COMMAND ----------

df_sorted = df_sorted.withColumnRenamed('code','athlete_id')

# COMMAND ----------

display(df_sorted)

# COMMAND ----------

# MAGIC %md
# MAGIC ### spliting the data

# COMMAND ----------

df_sorted = df_sorted.withColumn("occupation",split('occupation',','))

# COMMAND ----------

display(df_sorted)

# COMMAND ----------

df_ath.columns

# COMMAND ----------

df_sorted.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### select function

# COMMAND ----------

df_final = df_sorted.select('athlete_id',
 'current',
 'name',
 'name_short',
 'name_tv',
 'gender',
 'function',
 'country_code',
 'country',
 'country_long',
 'nationality',
 'nationality_long',
 'nationality_code',
 'height',
 'weight',
 'disciplines',
 'events',
 'birth_date',
 'birth_place',
 'birth_country',
 'residence_place',
 'residence_country',
 'nickname',
 'hobbies',
 'occupation',
 'education',
 'family',
 'lang',
 'coach',
 'reason',
 'hero',
 'influence',
 'philosophy',
 'sporting_relatives',
 'ritual',
 'other_sports')

# COMMAND ----------

display(df_final)

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ### window function

# COMMAND ----------

df_final.withColumn("cum_weight",sum("weight").over(Window.partitionBy("nationality").orderBy("height").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### TempView

# COMMAND ----------

df_final.createOrReplaceTempView("athletes")

# COMMAND ----------

# MAGIC %md
# MAGIC ### sql function

# COMMAND ----------

df_new = spark.sql(""" 
            select sum(weight) over(partition By nationality order By height rows between unbounded preceding and current row)
            From athletes
""")

# COMMAND ----------

display(df_new)

# COMMAND ----------

# MAGIC %md
# MAGIC ### writing the data to the silver container

# COMMAND ----------

df_final.write.format("delta")\
            .mode("append")\
            .option("path","abfss://silver@projcicddevaish242.dfs.core.windows.net/athletes")\
            .saveAsTable("olympics.silver.athletes")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * From olympics.silver.athletes

# COMMAND ----------

