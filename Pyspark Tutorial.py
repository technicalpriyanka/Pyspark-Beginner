# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

dbutils.fs.ls('/FileStore/tables')

# COMMAND ----------

df = spark.read.format('csv').option('inferSchema', True).option('header', True).load('/FileStore/tables/Bigmart_Sales.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Reading JSON

# COMMAND ----------

df_json = spark.read.format('json').option('inferSchema', True)\
                        .option('header', True)\
                        .option('multiLine', False)\
                        .load('/FileStore/tables/drivers.json')

# COMMAND ----------

df_json.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Schema Definition

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### DDL Schema

# COMMAND ----------

# #want to convert datatype-  double into string
# Item_Weight: double (nullable = true)

# COMMAND ----------

my_ddl_schema = '''
                    Item_identifier STRING,
                    Item_Weight STRING,
                    Item_Fat_Content STRING,
                    Item_Visibility DOUBLE,
                    Item_Type STRING,
                    Item_MRP DOUBLE,
                    Outlet_Identifier STRING,
                    Outlet_Establishment_Year INT,
                    Outlet_Size STRING,
                    Outlet_Location_Type STRING,
                    Outlet_Type STRING,
                    Item_Outlet_Sales DOUBLE
                '''


# COMMAND ----------

df = spark.read.format('csv')\
            .schema(my_ddl_schema)\
            .option('header', True)\
            .load('/FileStore/tables/Bigmart_Sales.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC StructType() Schema

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

my_struct_schema = StructType([
                                StructField('Item_identifier', StringType(), True),
                                StructField('Item_Weight', StringType(), True),
                                StructField('Item_Fat_Content', StringType(), True),
                                StructField('Item_Visibility', StringType(), True),
                                StructField('Item_Type', StringType(), True),
                                StructField('Item_MRP', StringType(), True),
                                StructField('Outlet_Identifier', StringType(), True),
                                StructField('Outlet_Establishment_Year', StringType(), True),
                                StructField('Outlet_Size', StringType(), True),
                                StructField('Outlet_Location_Type', StringType(), True),
                                StructField('Outlet_Type', StringType(), True),
                                StructField('Item_Outlet_Sales', StringType(), True),
])

# COMMAND ----------

df = spark.read.format('csv')\
                        .schema(my_struct_schema)\
                        .option('header', True)\
                        .load('/FileStore/tables/Bigmart_Sales.csv')
                        

# COMMAND ----------

df.display()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC SELECT

# COMMAND ----------

df.display()

# COMMAND ----------

df_select = df.select('Item_Identifier','Item_Weight','Item_Fat_Content').display()

# COMMAND ----------

df_select = df.select(col('Item_Identifier'),col('Item_Weight'),col('Item_Fat_Content'))

# COMMAND ----------

df_select.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### ALIAS

# COMMAND ----------

df.select(col('Item_Identifier').alias('Item_ID')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### FILTER/WHERE

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 1

# COMMAND ----------

#only regular
df.filter(col('Item_Fat_Content')=='Regular').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2

# COMMAND ----------

df.filter((col('Item_Type')=='Soft Drinks') & (col('Item_Weight')<10)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario 3

# COMMAND ----------

df.filter((col('Outlet_Size').isNull()) & (col('Outlet_Location_Type').isin('Tier 1', 'Tier 2'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###withColumnRenamed

# COMMAND ----------

df.withColumnRenamed('Item_Weight', 'Item_wt').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###withColumn

# COMMAND ----------

#modify existing one column and add new column 

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario 1

# COMMAND ----------

df = df.withColumn('flag', lit('new'))  #create new column

# COMMAND ----------

df.display()

# COMMAND ----------

df.withColumn('multiply', col('Item_Weight')*col('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario 2

# COMMAND ----------

df.withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),"Regular", "Reg"))\
    .withColumn('Item_Fat_Content', regexp_replace(col('Item_Fat_Content'),"Low Fat", "LF")).display()          #modify column

# COMMAND ----------

# MAGIC %md
# MAGIC ###Type Casting

# COMMAND ----------

#conversion of datatype

# COMMAND ----------

df = df.withColumn('Item_Weight', col('Item_Weight').cast(StringType()))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Sort/Order by

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario 1

# COMMAND ----------

df.sort(col('Item_weight').desc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario 2

# COMMAND ----------

df.sort(col('Item_Visibility').asc()).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 3

# COMMAND ----------

df.sort(['Item_Weight', 'Item_Visibility'], ascending = [0,0]).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 4

# COMMAND ----------

df.sort(['Item_Weight', 'Item_Visibility'], ascending = [0,1]).display()        #0 1 false true decending and acending order

# COMMAND ----------

# MAGIC %md
# MAGIC ###LIMIT

# COMMAND ----------

df.limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Drop

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario 1

# COMMAND ----------

df.drop('Item_Visibility').display()          #dropping single column

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2

# COMMAND ----------

df.drop('Item_Visibility', 'Item_Type').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Drop_Duplicates

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario 1

# COMMAND ----------

#row having duplicates

# COMMAND ----------

df.dropDuplicates().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scenario 2

# COMMAND ----------

df.drop_duplicates(subset=['Item_Type']).display()

# COMMAND ----------

df.distinct().display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###UNION and UNION BYNAME

# COMMAND ----------

# MAGIC %md
# MAGIC ###Preparing Dataframes

# COMMAND ----------

data1 = [('1','kad'),
         ('2','sid')]

schema1 = 'id STRING, name STRING'

df1 = spark.createDataFrame(data1,schema1)

data2 = [('3','rahul'),
         ('4','jas')]

schema2 = 'id STRING, name STRING' 

df2 = spark.createDataFrame(data2,schema2)

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Union

# COMMAND ----------

df1.union(df2).display()

# COMMAND ----------

data1 = [('kad','1'),
         ('sid','2')]

schema1 = 'name STRING, id STRING'

df1 = spark.createDataFrame(data1,schema1)

# COMMAND ----------

df1.display()

# COMMAND ----------

df1.union(df2).display()    #here data is messed up

# COMMAND ----------

# MAGIC %md
# MAGIC ### Union Byname

# COMMAND ----------

 df1.unionByName(df2).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###String Functions
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ###INITCAP() LOWER() UPPER()

# COMMAND ----------

df.select(initcap('Item_Type')).display()

# COMMAND ----------

df.select(lower('Item_Type')).display()

# COMMAND ----------

df.select(upper('Item_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DATE_FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ###Current_Dtae() date_add() date_sub()

# COMMAND ----------

df = df.withColumn('Current_date', current_date())

# COMMAND ----------

df.display()

# COMMAND ----------

df = df = df.withColumn('Week_after', date_add('Current_date', 7))

# COMMAND ----------

df.display()

# COMMAND ----------

df = df.withColumn('week_before', date_sub('Current_date', 7))

# COMMAND ----------

df.display()

# COMMAND ----------

 df = df = df.withColumn('week_before', date_add('Current_date', -7))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DATEDIFF

# COMMAND ----------

df.withColumn("datediff", datediff('Current_date', 'Week_after')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###DATE_FORMAT

# COMMAND ----------

df = df.withColumn('week_before', date_format('week_before', 'dd-MM-yyyy'))

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling NULLS

# COMMAND ----------

# MAGIC %md
# MAGIC ###Dropping nulls

# COMMAND ----------

df.dropna('all').display()

# COMMAND ----------

# df.dropna('any').display()

# COMMAND ----------

df.display()

# COMMAND ----------

df.dropna(subset=['Outlet_Size']).display()

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Filling nulls

# COMMAND ----------

df.fillna('NotAvailable').display()

# COMMAND ----------

df.fillna('NotAvailable', subset=['Outlet_Size']).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###SPLIT and Indexing

# COMMAND ----------

df.withColumn('Outlet_Size', split('Outlet_Type', ' ')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Indexing

# COMMAND ----------

df.withColumn('Outlet_Size', split('Outlet_Type', ' ')[1]).display()      #indexing taking only 1st index

# COMMAND ----------

# MAGIC %md
# MAGIC ###EXPLODE

# COMMAND ----------

df_exp = df.withColumn('Outlet_Type', split('Outlet_Type', ' '))

df_exp.display()

# COMMAND ----------

df_exp.withColumn('Outlet_Type', explode('Outlet_Type')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###ARRAY_CONTAINS

# COMMAND ----------

df_exp.withColumn('Type1_flag', array_contains('Outlet_Type','Type1')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###GROUP_BY

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario 1

# COMMAND ----------

df_exp.groupBy('Item_Type').agg(sum('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario 2

# COMMAND ----------

df_exp.groupBy('Item_Type').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario 3

# COMMAND ----------

df_exp.groupBy('Item_Type', 'Outlet_Size').agg(avg('Item_MRP').alias('Total MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario 4

# COMMAND ----------

df_exp.groupBy('Item_Type').agg(sum('Item_MRP'), avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###COLLECT_LIST

# COMMAND ----------

data = [('user1','book1'),
        ('user1','book2'),
        ('user2','book2'),
        ('user2','book4'),
        ('user3','book1')]
    
schema = 'user string, book string'
    
df_book = spark.createDataFrame(data,schema)
df_book.display()

# COMMAND ----------

df_book.groupBy('user').agg(collect_list('book')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###PIVOT

# COMMAND ----------

df.groupBy('Item_Type').pivot('Outlet_Size').agg(avg('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###WHEN_OTHERWISE

# COMMAND ----------

#conditional columns

# COMMAND ----------

df_exp=df.withColumn('veg_flag', when(col('Item_Type')=='Meat', 'Non-veg').otherwise('veg'))
df_exp.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Scenario 2

# COMMAND ----------

df_exp.withColumn('veg_exp_flag', when(((col('veg_flag')=='veg') & (col('Item_MRP')<100)), 'Veg_Inexpensive')\
            .when(((col('veg_flag')=='veg') & (col('Item_MRP')>100)), 'Veg_Expensive')\
            .otherwise('Non_veg')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###JOINS

# COMMAND ----------

# MAGIC %md
# MAGIC ###INNER JOIN

# COMMAND ----------

dataj1 = [('1','gaur','d01'),
            ('2','kit','d02'),
            ('3','sam','d03'),
            ('4','tim','d03'),
            ('5','aman','d05'),
            ('6','nad','d06')]
    
schemaj1 = 'emp_id STRING, emp_name STRING, dept_id STRING'

df1 = spark.createDataFrame(dataj1,schemaj1)

dataj2 = [('d01','HR'),
        ('d02','Marketing'),
        ('d03','Accounts'),
        ('d04','IT'),
        ('d05','Finance')]

schemaj2 = 'dept_id STRING, department STRING'

df2 = spark.createDataFrame(dataj2,schemaj2)
   

# COMMAND ----------

df1.display()

# COMMAND ----------

df2.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###inner join

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'inner').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###LEFT JOIN

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'left').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###RIGHT JOIN

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'right').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###ANTI JOIN

# COMMAND ----------

#records are not matching in both table

# COMMAND ----------

df1.join(df2, df1['dept_id'] == df2['dept_id'], 'anti').display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###WINDOW FUNCTIONS()

# COMMAND ----------

# MAGIC %md
# MAGIC ###ROW NUMBER()

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

df.withColumn('rowCol', row_number().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### RANK() and DENSE_RANK()

# COMMAND ----------

df.withColumn('rank', rank().over(Window.orderBy('Item_Identifier'))).display()

# COMMAND ----------

df.withColumn('rank', rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

df.withColumn('rank', rank().over(Window.orderBy(col('Item_Identifier').desc())))\
    .withColumn('denseRank', dense_rank().over(Window.orderBy(col('Item_Identifier').desc()))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Cumulative Sum

# COMMAND ----------

df.withColumn('cumsum', sum('Item_MRP').over(Window.orderBy('Item_Type'))).display()

# COMMAND ----------

df.withColumn('cumsum', sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.currentRow))).display()

# COMMAND ----------

df.withColumn('cumsum', sum('Item_MRP').over(Window.orderBy('Item_Type').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### USER DEFINED FUNCTIONS

# COMMAND ----------

# MAGIC %md
# MAGIC ###STEP 1

# COMMAND ----------

def my_func(x):
    return x*x

# COMMAND ----------

# MAGIC %md
# MAGIC ###STEP 2

# COMMAND ----------

my_udf = udf(my_func)

# COMMAND ----------

# MAGIC %md
# MAGIC ### STEP 3

# COMMAND ----------

df.withColumn('mynewcol', my_udf('Item_MRP')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Writing

# COMMAND ----------

# MAGIC %md
# MAGIC ###csv

# COMMAND ----------

df.write.format('csv')\
        .save('/FileStore/tables/csv/data.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data writing Modes

# COMMAND ----------

# MAGIC %md
# MAGIC ###APPEND

# COMMAND ----------

df.write.format('csv')\
        .mode('append')\
        .option('path','/FileStore/tables/csv/data.csv')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###OVERWRITE

# COMMAND ----------

df.write.format('csv')\
        .mode('overwrite')\
        .option('path','/FileStore/tables/csv/data.csv')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###ERROR

# COMMAND ----------

df.write.format('csv')\
        .mode('error')\
        .option('path','/FileStore/tables/csv/data.csv')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###IGNORE

# COMMAND ----------

df.write.format('csv')\
        .mode('ignore')\
        .option('path','/FileStore/tables/csv/data.csv')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Parquet File Format

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row Based

# COMMAND ----------

# MAGIC %md
# MAGIC ###Columnar

# COMMAND ----------

df.write.format('parquet')\
        .mode('append')\
        .option('path','/FileStore/tables/csv/data.csv')\
        .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ###TABLE

# COMMAND ----------

df.write.format('csv')\
        .mode('overwrite')\
        .saveAsTable('mytable')

# COMMAND ----------

# MAGIC %md
# MAGIC ### MANAGED Vs EXTERNAL TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC ###SPARK SQL

# COMMAND ----------

### CreateTempView

# COMMAND ----------

df.createTempView('my_view')

# COMMAND ----------

select * from my_view where Item_Fat_Content = 'LF'

# COMMAND ----------

df_sql = spark.sql("select * from my_view where Item_Fat_Content = 'LF'")

# COMMAND ----------

df_sql.display()

# COMMAND ----------

