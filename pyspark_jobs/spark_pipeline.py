from __future__ import annotations

from pathlib import Path

# SparkSession
# Ez a Spark belépési pontja. Nagyjából olyan, mint pandasnál a környezet fő objektuma.
# Ezzel tudunk:
#  - fájlt olvasni
#  - DataFrame-et létrehozni
#  - Spark műveleteket futtatni

# DataFrame
# Type hintként használjuk, hogy jelezzük: egy függvény Spark DataFrame-et kap vagy ad vissza.
from pyspark.sql import SparkSession, DataFrame

# Spark SQL függvények
# col: Egy oszlopra hivatkozunk vele Spark-kifejezésként.
# count: Aggregációhoz kell.
# lit: Konstans értéket csinál Spark oszlopból.
# sum: Aggregációhoz kell.
# try_to_date: Biztonságos dátum parse.
from pyspark.sql.functions import col, count, lit, sum, try_to_date

# StructType, StructField
# Ezekkel explicit módon leírjuk az input CSV szerkezetét.

# StringType, IntegerType, DoubleType
# Spark adattípusok.
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType


def get_project_root() -> Path:
    return Path(__file__).resolve().parent.parent


def get_raw_data_dir() -> Path:
    return get_project_root() / "data" / "raw"


def get_processed_dir() -> Path:
    return get_project_root() / "data" / "processed"


def get_rejected_dir() -> Path:
    return get_project_root() / "data" / "rejected"

# Ez hozza létre a Spark környezetet.
def create_spark_session() -> SparkSession:
    return (
        # Builder minta, ezzel konfiguráljuk a sessiont.
        SparkSession.builder
        # Elnevezi a Spark alkalmazást. Ez logokban és UI-ban hasznos.
        .appName("RetailDataQualitySparkPipeline")
        # Azt mondja: helyi gépen fusson, és használja az összes elérhető CPU magot.
        # Az * azt jelenti, hogy a Spark a helyi gép erőforrásain dolgozik, nem clusteren.
        .master("local[*]")
        # Ha már van session, azt használja, különben létrehozza.
        .getOrCreate()
    )

# Bemenet: Spark session
# Kimenet: orders Spark DataFrame
def read_orders_raw(spark: SparkSession) -> DataFrame:
    # Ez explicit leírja az orders CSV szerkezetét.
    # Azért String mindegyik, hogy a raw data megmaradjon
    # True: Az oszlop nullable, vagyis lehet benne null.
    orders_schema = StructType(
        [
            StructField("order_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("product_id", StringType(), True),
            StructField("order_date", StringType(), True),
            StructField("quantity", StringType(), True),
            StructField("total_amount", StringType(), True),
            StructField("currency", StringType(), True),
        ]
    )
    # Beolvasás
    return (
        # Spark DataFrame olvasó.
        spark.read
        # A CSV első sora fejléc.
        .option("header", True)
        # Nem inferálunk sémát, hanem kézzel megmondjuk.
        .schema(orders_schema)
        .csv(str(get_raw_data_dir() / "orders.csv"))
    )


def read_customers_raw(spark: SparkSession) -> DataFrame:
    customers_schema = StructType(
        [
            StructField("customer_id", StringType(), True),
            StructField("full_name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("country", StringType(), True),
            StructField("registration_date", StringType(), True),
        ]
    )

    return (
        spark.read
        .option("header", True)
        .schema(customers_schema)
        .csv(str(get_raw_data_dir() / "customers.csv"))
    )


def read_products_raw(spark: SparkSession) -> DataFrame:
    products_schema = StructType(
        [
            StructField("product_id", StringType(), True),
            StructField("product_name", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", StringType(), True),
        ]
    )

    return (
        spark.read
        .option("header", True)
        .schema(products_schema)
        .csv(str(get_raw_data_dir() / "products.csv"))
    )

# Bemenet: raw orders Spark DataFrame
# Kimenet: kibővített DataFrame új, típusosított oszlopokkal
# Nem felülírjuk a raw oszlopokat, hanem új oszlopokat hozunk létre.
def transform_orders(orders_df: DataFrame) -> DataFrame:
    return (
        orders_df
        # withColumn(...)
        # Új oszlopot hoz létre vagy meglévőt felülír.
        # cast(IntegerType())
        # Stringből int próbál készülni. Ha nem lehet, akkor általában NULL.
        # try_to_date(...)
        # Ez parse-olja a dátumot a megadott formátumban, de hibás inputnál NULL.
        .withColumn("order_id_int", col("order_id").cast(IntegerType()))
        .withColumn("customer_id_int", col("customer_id").cast(IntegerType()))
        .withColumn("product_id_int", col("product_id").cast(IntegerType()))
        .withColumn("order_date_parsed", try_to_date(col("order_date"), "yyyy-MM-dd"))
        .withColumn("quantity_double", col("quantity").cast(DoubleType()))
        .withColumn("total_amount_double", col("total_amount").cast(DoubleType()))
    )


def transform_customers(customers_df: DataFrame) -> DataFrame:
    return (
        customers_df
        .withColumn("customer_id_int", col("customer_id").cast(IntegerType()))
    )


def transform_products(products_df: DataFrame) -> DataFrame:
    return (
        products_df
        .withColumn("product_id_int", col("product_id").cast(IntegerType()))
        .withColumn("price_double", col("price").cast(DoubleType()))
    )

# Cél: megtalálni az orders sorokat, ahol a customer_id nem létezik a customers táblában.
def build_invalid_customer_fk_df(orders_df: DataFrame, customers_df: DataFrame) -> DataFrame:
    # Kiválasztja a customers DataFrame-ből a customer_id oszlopot, és eldobja a duplikátumokat.
    valid_customers_df = customers_df.select("customer_id").dropDuplicates()

    return (
        # left_anti join
        # A bal oldali táblából azokat a sorokat adja vissza, amelyekhez nincs találat a jobb oldalon.
        orders_df.join(valid_customers_df, on="customer_id", how="left_anti")
        # lit("invalid_customer_fk")
        # Hozzáadja a rejection okot.
        .withColumn("rejection_reason", lit("invalid_customer_fk"))
    )


def build_invalid_product_fk_df(orders_df: DataFrame, products_df: DataFrame) -> DataFrame:
    valid_products_df = products_df.select("product_id").dropDuplicates()

    return (
        orders_df.join(valid_products_df, on="product_id", how="left_anti")
        .withColumn("rejection_reason", lit("invalid_product_fk"))
    )

# Itt gyűjtjük össze az összes rejected szabályt.
def build_rejected_orders(
    orders_df: DataFrame,
    customers_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    
    # Duplikált order ID-k
    duplicate_order_ids = (
        # order_id szerint csoportosít
        orders_df.groupBy("order_id")
        # count("*"): megszámolja, hány sor van adott order_id-hoz
        .agg(count("*").alias("row_count"))
        # filter(row_count > 1): csak a duplikált order_id-k maradnak
        .filter(col("row_count") > 1)
        # select("order_id"): csak az azonosító kell
        .select("order_id")
    )

    duplicated_orders_df = (
        # left_semi
        # A bal oldali táblából csak azokat a sorokat adja vissza, amelyekhez van találat a jobb oldalon.
        # Tehát:
        #  - ha egy order_id szerepel a duplikált listában
        #  - akkor az összes ilyen order sor visszajön
        orders_df.join(duplicate_order_ids, on="order_id", how="left_semi")
        .withColumn("rejection_reason", lit("duplicate_order_id"))
    )

    # Olyan sorokat keres, ahol:
    #  - a quantity nem parse-olható számmá
    #  - vagy kisebb mint 1
    invalid_quantity_df = (
        orders_df
        .filter(col("quantity_double").isNull() | (col("quantity_double") < 1))
        .withColumn("rejection_reason", lit("invalid_quantity"))
    )

    invalid_total_amount_df = (
        orders_df
        .filter(col("total_amount_double").isNull() | (col("total_amount_double") < 0))
        .withColumn("rejection_reason", lit("invalid_total_amount"))
    )

    invalid_order_date_df = (
        orders_df
        .filter(col("order_date").isNotNull() & col("order_date_parsed").isNull())
        .withColumn("rejection_reason", lit("invalid_order_date"))
    )

    invalid_customer_fk_df = build_invalid_customer_fk_df(orders_df, customers_df)
    invalid_product_fk_df = build_invalid_product_fk_df(orders_df, products_df)

    # Minden rejected issue összefűzése
    # unionByName
    # Egymás alá fűzi a DataFrame-eket az oszlopnevek alapján.
    rejected_df = (
        duplicated_orders_df
        .unionByName(invalid_quantity_df)
        .unionByName(invalid_total_amount_df)
        .unionByName(invalid_order_date_df)
        .unionByName(invalid_customer_fk_df)
        .unionByName(invalid_product_fk_df)
        .dropDuplicates(["order_id", "rejection_reason"])
    )

    return rejected_df


def build_clean_orders(orders_df: DataFrame, rejected_df: DataFrame) -> DataFrame:
    # Összegyűjti azoknak az order-eknek az ID-ját, amelyek valamilyen rejected okkal szerepelnek.
    rejected_order_ids = rejected_df.select("order_id").dropDuplicates()

    # Ez visszaadja azokat az order rekordokat, amelyek order_id-ja nincs benne a rejected listában.
    clean_df = (
        orders_df.join(rejected_order_ids, on="order_id", how="left_anti")
    )

    return clean_df

# Naponként csoportosít, majd kiszámolja:
#  - rendelés darabszám
#  - összes mennyiség
#  - összes bevétel
def build_daily_sales_kpi(clean_orders_df: DataFrame) -> DataFrame:
    return (
        clean_orders_df
        .groupBy("order_date_parsed")
        .agg(
            count("order_id").alias("order_count"),
            sum("quantity_double").alias("total_quantity"),
            sum("total_amount_double").alias("total_revenue"),
        )
        .orderBy("order_date_parsed")
    )


def build_category_sales_summary(
    clean_orders_df: DataFrame,
    products_df: DataFrame,
) -> DataFrame:
    return (
        clean_orders_df
        .join(products_df.select("product_id", "category"), on="product_id", how="inner")
        .groupBy("category")
        .agg(
            count("order_id").alias("order_count"),
            sum("quantity_double").alias("total_quantity"),
            sum("total_amount_double").alias("total_revenue"),
        )
        .orderBy(col("total_revenue").desc())
    )


def build_rejection_reason_summary(rejected_orders_df: DataFrame) -> DataFrame:
    return (
        rejected_orders_df
        .groupBy("rejection_reason")
        .agg(
            count("order_id").alias("rejected_record_count"),
        )
        .orderBy(col("rejected_record_count").desc())
    )

# Ment egy clean és egy rejected Spark CSV outputot.
def save_orders_outputs(clean_df: DataFrame, rejected_df: DataFrame) -> None:
    processed_output = str(get_processed_dir() / "spark_clean_orders")
    rejected_output = str(get_rejected_dir() / "spark_rejected_orders")

    clean_df.write.mode("overwrite").option("header", True).csv(processed_output)
    rejected_df.write.mode("overwrite").option("header", True).csv(rejected_output)


def save_kpi_outputs(
    daily_sales_kpi_df: DataFrame,
    category_sales_summary_df: DataFrame,
    rejection_reason_summary_df: DataFrame,
) -> None:
    daily_output = str(get_processed_dir() / "spark_daily_sales_kpi")
    category_output = str(get_processed_dir() / "spark_category_sales_summary")
    rejection_output = str(get_processed_dir() / "spark_rejection_reason_summary")

    daily_sales_kpi_df.write.mode("overwrite").option("header", True).csv(daily_output)
    category_sales_summary_df.write.mode("overwrite").option("header", True).csv(category_output)
    rejection_reason_summary_df.write.mode("overwrite").option("header", True).csv(rejection_output)


def main() -> None:
    # Létrehozza a Spark sessiont.
    spark = create_spark_session()

    try:
        # Raw beolvasás
        # Mindhárom raw CSV-ből Spark DataFrame készül.
        orders_raw_df = read_orders_raw(spark)
        customers_raw_df = read_customers_raw(spark)
        products_raw_df = read_products_raw(spark)

        # Transzformáció
        # Ez még nem hajtja végre feltétlenül a műveleteket azonnal, csak felépíti a Spark logikai tervét.
        orders_transformed_df = transform_orders(orders_raw_df)
        customers_transformed_df = transform_customers(customers_raw_df)
        products_transformed_df = transform_products(products_raw_df)

        rejected_orders_df = build_rejected_orders(
            orders_transformed_df,
            customers_transformed_df,
            products_transformed_df,
        )
        clean_orders_df = build_clean_orders(orders_transformed_df, rejected_orders_df)

        save_orders_outputs(clean_orders_df, rejected_orders_df)

        daily_sales_kpi_df = build_daily_sales_kpi(clean_orders_df)
        category_sales_summary_df = build_category_sales_summary(
            clean_orders_df,
            products_transformed_df,
        )
        rejection_reason_summary_df = build_rejection_reason_summary(rejected_orders_df)

        save_kpi_outputs(
            daily_sales_kpi_df,
            category_sales_summary_df,
            rejection_reason_summary_df,
        )

        # A count() is akció.
        # Ez valóban megszámolja a sorokat, és ez is végrehajtatja a Spark tervet.
        print("Spark pipeline finished successfully.")
        print(f"Clean rows: {clean_orders_df.count()}")
        print(f"Rejected rows: {rejected_orders_df.count()}")

    finally:
        spark.stop()

if __name__ == "__main__":
    main()
