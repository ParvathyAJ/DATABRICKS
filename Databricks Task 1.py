# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Databricks Task 1
# MAGIC
# MAGIC 1. Upload given csv file and find the average salary for each gender.
# MAGIC 2. Using Pyspark select employees from Thrissur and salary > 32000.
# MAGIC 3. Add a new column named ‘Bonus’, where the salary increased by 5000 for all employees.
# MAGIC 4. Delete the place column and save this in a new dataframe.
# MAGIC
# MAGIC ## Data to be loaded
# MAGIC
# MAGIC /FileStore/tables/EmployeeData.csv

# COMMAND ----------

# Read the csv file 

emp_df = spark.read\
.option("inferSchema",True)\
.option("Header", True)\
.csv("/FileStore/tables/EmployeeData.csv")

display(emp_df)

# COMMAND ----------

emp_df.printSchema()

# COMMAND ----------

#  Find the average salary for each gender

from pyspark.sql.functions import avg

Avg_sal_by_gender = emp_df.groupBy("GENDER").avg("SALARY").withColumnRenamed("avg(SALARY)","Average_Salary_By_Gender")
display(Avg_sal_by_gender)


# COMMAND ----------

# Select employees from Thrissur and salary > 32000.

emp_filt = emp_df.filter("PLACE = 'Thrissur' and SALARY > 32000")
display(emp_filt)

# COMMAND ----------

# Add a new column named ‘Bonus’, where the salary increased by 5000 for all employees.
from pyspark.sql.functions import col

df_new = emp_df.withColumn("Bonus", col("SALARY") + 5000)
display(df_new)


# COMMAND ----------

# Delete the place column and save this in a new dataframe.

new_df = emp_df.drop('PLACE')
display(new_df)


# COMMAND ----------


