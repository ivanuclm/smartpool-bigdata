#!/usr/bin/env python
# coding: utf-8

# In[1]:


from smartpool_config import *

spark = create_spark("smartpool-arquitecture")

print("Spark OK:", spark.version)
print("BASE:", BASE)
print("STATE:", STATE)


# In[2]:


df_pools = (
    spark.read
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", "dbo.pools_dim")
    .option("user", JDBC_USER)
    .option("password", JDBC_PASS)
    .option("driver", JDBC_DRIVER)
    .load()
)

print("Pools Dim en SQL Server:")
df_pools.printSchema()
df_pools.show()


# In[3]:


bronze_pools_path = f"{BRONZE}/pools_dim"


# In[4]:


# (
#     df_pools
#     .write
#     .format("delta")
#     .mode("overwrite")
#     .option("overwriteSchema", "true")
#     .save(bronze_pools_path)
# )

# print("Escrito en:", bronze_pools_path)


# In[5]:


try:
    df_pools_bronze = (
        spark.read
        .format("delta")
        .load(bronze_pools_path)
    )
    print("Pools Dim en Bronze:")
    df_pools_bronze.show(5, truncate=False)
    df_heated = df_pools_bronze.filter("is_heated = true")
    df_heated.show()
except Exception as e:
    # print(f"Bronze pools_dim aún no existe en {bronze_pools_path}: {e}")
    print(f"Bronze pools_dim aún no existe en {bronze_pools_path}")
    


# In[6]:


df_maint = (
    spark.read
    .format("jdbc")
    .option("url", JDBC_URL)
    .option("dbtable", "dbo.maintenance_events")
    .option("user", JDBC_USER)
    .option("password", JDBC_PASS)
    .option("driver", JDBC_DRIVER)
    .load()
)

print("Registros en SQL Server de maintenance_events:", df_maint.count())
df_maint.show()
df_maint.printSchema()


# In[7]:


bronze_maint_path = f"{BRONZE}/maintenance_events"


# In[8]:


# (
#     df_maint
#     .write
#     .format("delta")
#     .mode("overwrite")  # full load inicial
#     .option("overwriteSchema", "true")
#     .save(bronze_maint_path)
# )

# print("Maintenance events escritos en:", bronze_maint_path)


# In[ ]:


# Solo lectura de Bronze
try:
    df_maint_bronze = (
        spark.read
        .format("delta")
        .load(bronze_maint_path)
    )

    print("Maintenance events en Bronze:")
    df_maint_bronze.show()
    df_maint_bronze.printSchema()
    print("Registros en Bronze:", df_maint_bronze.count())

    df_join = (
        df_maint_bronze.alias("m")
        .join(df_pools_bronze.alias("p"), F.col("m.pool_id") == F.col("p.pool_id"), "left")
    )
    
    df_join.select(
        "m.id",
        "m.event_time",
        "m.intervention_type",
        "m.product_type",
        "p.pool_name",
        "p.location",
        "p.is_heated"
    ).show(truncate=False)

except Exception as e:
    # print(f"Bronze maintenance_events aún no existe en {bronze_maint_path}: {e}")
    print(f"Bronze maintenance_events aún no existe en {bronze_maint_path}")
    


# In[10]:


spark.stop()

