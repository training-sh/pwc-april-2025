# use this file only if you did not make dlt from notebook
import dlt
from pyspark.sql.functions import col, lit, current_timestamp

username = dbutils.secrets.get(scope="sqlserver-creds", key="username")
password = dbutils.secrets.get(scope="sqlserver-creds", key="password")

DATABASE = "free-sql-db-5202703"
HOSTNAME = "gks-test.database.windows.net"

# f-string

jdbc_url = f"jdbc:sqlserver://{HOSTNAME}:1433;database={DATABASE};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

jdbc_properties = {
    "user": username ,
    "password": password ,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

@dlt.table(name="products_cdc_raw")
def read_cdc_from_sql():
    return spark.read.jdbc(
        url=jdbc_url,
        table="cdc.gks_products_CT",
        properties=jdbc_properties
    )

# Step 2: Filter only insert/update rows, and add last_updated timestamp
@dlt.table
def products_cdc_clean():
    return (
        dlt.read("products_cdc_raw")
        .filter(col("__$operation").isin(2, 4))  # 2 = insert, 4 = update after
        .select(
            col("id"),
            col("name"),
            col("price"),
            col("stock"),
            current_timestamp().alias("last_updated")
        )
    )


dlt.create_target_table('products_scd1', 
                        table_properties = {'delta.enableChangeDataFeed': 'true'})

# Step 3: Apply SCD Type 1 using apply_changes (merge by id)
dlt.apply_changes(
    target = "products_scd1",
    source = "products_cdc_clean",
    keys = ["id"],
    sequence_by = col("last_updated"),
    
    except_column_list = [],   # All columns are overwritten
    stored_as_scd_type = 1
)


# Step 2: Filter only insert/update rows, and add last_updated timestamp
# SCD 2 maintain history of changes
@dlt.table
def products_cdc_clean_scd2():
    return (
        dlt.read("products_cdc_raw")
        .filter(col("__$operation").isin(2, 4))  # 2 = insert, 4 = update after
        .select(
            col("id"),
            col("name"),
            col("price"),
            col("stock"),
            col("__$seqval").alias("sequenceNum")
        )
    ) 


dlt.create_target_table('products_scd2', 
                        table_properties = {'delta.enableChangeDataFeed': 'true'})

dlt.apply_changes(
    target="products_scd2",
    source="products_cdc_clean_scd2",
    keys=["id"],
    sequence_by=col("sequenceNum"),
    except_column_list=["sequenceNum"],      # overwrite all
    stored_as_scd_type= 2 ,       # <- this is the key difference!
)