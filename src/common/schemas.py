from pyspark.sql.types import StructField , StructType , StringType, IntegerType, DateType

CUST_INFO_SCHEMA = StructType([
    StructField("cst_id", IntegerType(), True),
    StructField("cst_key", StringType(), True),
    StructField("cst_firstname", StringType(), True),
    StructField("cst_lastname", StringType(), True),
    StructField("cst_marital_status", StringType(), True),
    StructField("cst_gndr", StringType(), True),
    StructField("cst_create_date", DateType(), True)
])


PRD_INFO_SCHEMA = StructType([
    StructField("prd_id", IntegerType(), True),
    StructField("prd_key", StringType(), True),
    StructField("prd_nm", StringType(), True),
    StructField("prd_cost", IntegerType(), True),
    StructField("prd_line", StringType(), True),
    StructField("prd_start_dt", DateType(), True),
    StructField("prd_end_dt", DateType(), True)
])


SALES_DETAILS_SCHEMA = StructType([
    StructField("sls_ord_num", StringType(), True),
    StructField("sls_prd_key", StringType(), True),
    StructField("sls_cust_id", IntegerType(), True),
    StructField("sls_order_dt", IntegerType(), True),
    StructField("sls_ship_dt", IntegerType(), True),
    StructField("sls_due_dt", IntegerType(), True),
    StructField("sls_sales", IntegerType(), True),
    StructField("sls_quantity", IntegerType(), True),
    StructField("sls_price", IntegerType(), True)
])


LOC_A101_SCHEMA = StructType([
    StructField("cid", StringType(), True),
    StructField("cntry", StringType(), True)
])

CUST_AZ12_SCHEMA = StructType([
    StructField("cid", StringType(), True),
    StructField("bdate", DateType(), True),
    StructField("gen", StringType(), True)
])


PX_CAT_G1V2_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("cat", StringType(), True),
    StructField("subcat", StringType(), True),
    StructField("maintenance", StringType(), True)
])
