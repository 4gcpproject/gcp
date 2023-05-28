from pyspark.sql import SparkSession

#GCS to BQ of fact_table
spark = SparkSession.builder.appName("CSV fact to BQ").getOrCreate()
 
dataframe = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("gs://just_bucket4/2023-05-24-19-56/sales_product_customer_outlet-r-00000")
 
selected_columns = ["transaction_id", "transaction_date", "transaction_time", "_sales_outlet_id_", "staff_id", "_customer_id_", 
                    "instore_yn", "order", "line_item_id", "_product_id_", "quantity", "line_item_amount", "unit_price", "promo_item_yn"]

selected_dataframe = dataframe.select(selected_columns)

table_name = "savvy-hull-383206.demo_dataset.fact_table"

selected_dataframe.write.format("bigquery").option("table", table_name).option("temporaryGcsBucket", "temp_bucket-4").mode("overwrite").save()

#GCS to BQ of dim_product
spark = SparkSession.builder.appName("CSV dim_product to BigQuery").getOrCreate()
 
dataframe = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("gs://just_bucket4/2023-05-24-19-56/sales_product_customer_outlet-r-00000")
 
selected_columns = ["_product_id_", "product_group", "product_category", "product_type", "product", "current_wholesale_price", 
                    "current_retail_price", "tax_exempt_yn", "promo_yn", "new_product_yn"]

selected_dataframe = dataframe.select(selected_columns)

selected_dataframe = selected_dataframe.distinct()

table_name = "savvy-hull-383206.demo_dataset.dim_product"

selected_dataframe.write.format("bigquery").option("table", table_name).option("temporaryGcsBucket", "temp_bucket-4").mode("overwrite").save()

#GCS to BQ of dim_customer
spark = SparkSession.builder.appName("CSV dim_customer to BigQuery").getOrCreate()
 
dataframe = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("gs://just_bucket4/2023-05-24-19-56/sales_product_customer_outlet-r-00000")
 
selected_columns = ["_customer_id_", "customer_first_name", "customer_email", 
                    "customer_since","loyalty_card_number", "birthdate", "gender", "birth_year"]

selected_dataframe = dataframe.select(selected_columns)

selected_dataframe = selected_dataframe.distinct()

table_name = "savvy-hull-383206.demo_dataset.dim_customer"

selected_dataframe.write.format("bigquery").option("table", table_name).option("temporaryGcsBucket", "temp_bucket-4").mode("overwrite").save()

#GCS to BQ of dim_sales_outlet
spark = SparkSession.builder.appName("CSV dim_sales_outlet to BigQuery").getOrCreate()
 
dataframe = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("gs://just_bucket4/2023-05-24-19-56/sales_product_customer_outlet-r-00000")
 
selected_columns = ["_sales_outlet_id_", "sales_outlet_type", "store_address", "store_city", 
                    "store_postal_code", "manager", "Neighorhood"]

selected_dataframe = dataframe.select(selected_columns)

selected_dataframe = selected_dataframe.distinct()

table_name = "savvy-hull-383206.demo_dataset.dim_sales_outlet"

selected_dataframe.write.format("bigquery").option("table", table_name).option("temporaryGcsBucket", "temp_bucket-4").mode("overwrite").save()



spark.stop()