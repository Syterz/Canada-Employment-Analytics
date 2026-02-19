import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import trim, col, to_date, year, lit
from pyspark.sql.types import DoubleType



logger = logging.getLogger()
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler()
    logger.addHandler(handler)


args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --------- CONFIG ----------
RAW_S3_NAICS_PATH = "s3://raw-files-canada-employment/NAICS Canada Data.csv"  
RAW_S3_Monthly_Vac_PATH = "s3://raw-files-canada-employment/Monthly Vac.csv"
RAW_S3_Quarterly_Vac_PATH = "s3://raw-files-canada-employment/Canada Quarterly/*.csv"

OUTPUT_S3_PATH_NAICS = "s3://raw-files-canada-employment/etl-finished-files/NAICS/"
OUTPUT_S3_PATH_Monthly = "s3://raw-files-canada-employment/etl-finished-files/MONTHLY/"
OUTPUT_S3_PATH_Quarterly = "s3://raw-files-canada-employment/etl-finished-files/QUARTERLY/"
# ---------------------------


def extract():
    """Load raw dataset"""
    df_NAICS = spark.read.option("header",True).csv(RAW_S3_NAICS_PATH)
    df_Monthly_Vac = spark.read.option("header",True).csv(RAW_S3_Monthly_Vac_PATH)

    # Extracting all the files in that one folder (Contains data on each pronvince, Territories and Canada as a total)
    df_Quarterly_Vac = spark.read.option("header",True).csv(RAW_S3_Quarterly_Vac_PATH)

    return df_NAICS, df_Monthly_Vac, df_Quarterly_Vac


def transform_NAICS(df):
    """Cleaning for NAICS"""
    
    # Standardize type of employment
    df = df.filter(col("Type of employee") == "All employees")

    Kept_columns = [
        'REF_DATE', 'GEO','North American Industry Classification System (NAICS)', 'VALUE'
    ]
    df = df.select(*[col(c) for c in Kept_columns])
    df = df.withColumn("VALUE", col("VALUE").cast(DoubleType()))
    
    # Trimming strings
    for c in ['REF_DATE', 'GEO', 'North American Industry Classification System (NAICS)']:
        df = df.withColumn(c, trim(col(c)))

    # Keeping only after 2015
    df = df.filter(col("REF_DATE") >= "2015-01")
    df = df.withColumn("Year", year(col("REF_DATE")))
    
    df = df.select(
        col("REF_DATE").alias("Date"),
        col("GEO"),
        col("North American Industry Classification System (NAICS)").alias("NAICS"),
        col("VALUE"),
        col("Year")
    )

    return df


def transform_Monthly_Vac(df):
    """Cleaning for Mothly Vacancy Dataset"""

    # Taking only the needed columns
    Kept_columns = ['REF_DATE','GEO','Statistics','VALUE']
    df = df.select(*[col(c) for c in Kept_columns])
    
    for c in ['REF_DATE', 'GEO','Statistics']:
        df = df.withColumn(c, trim(col(c)))
        
    df = df.withColumn("VALUE", col("VALUE").cast(DoubleType()))
    df = df.select(
        col("REF_DATE").alias("Date"),
        col("GEO"),
        col("Statistics"),
        col("VALUE")
    )
    df = df.withColumn("Year", year(to_date(col("Date"), "yyyy-MM")))

    return df


def transform_Quarterly_Vac(df):
    """Cleaning for Quarterly Vacancy Dataset which includes NAICS data"""
    Kept_columns = [
        'REF_DATE','GEO','North American Industry Classification System (NAICS)',
        'Statistics','VALUE'
    ]
    df = df.select(*[col(c) for c in Kept_columns])
    
    for c in ['REF_DATE', 'GEO','North American Industry Classification System (NAICS)','Statistics']:
        df = df.withColumn(c, trim(col(c)))
        
    df = df.withColumn("VALUE", col("VALUE").cast(DoubleType()))
    df = df.select(
        col("REF_DATE").alias("Date"),
        col("GEO"),
        col("North American Industry Classification System (NAICS)").alias("NAICS"),
        col("Statistics"),
        col("VALUE")
    )
    df = df.withColumn("Year", year(to_date(col("Date"), "yyyy-MM")))

    return df



def load(df_naics, df_monthly, df_quarterly):
    """Write analytics-ready dataset"""
    logger.info("Starting NAICS write")
    df_naics.write.mode("overwrite").partitionBy("Year","GEO").parquet(OUTPUT_S3_PATH_NAICS)
    logger.info("NAICS write completed")
    
    logger.info("Starting Monthly write")
    df_monthly.write.mode("overwrite").partitionBy("Year","GEO").parquet(OUTPUT_S3_PATH_Monthly)
    logger.info("Monthly write completed")
    
    logger.info("Starting Quarterly write")
    df_quarterly.write.mode("overwrite").partitionBy("Year","GEO").parquet(OUTPUT_S3_PATH_Quarterly)
    logger.info("Quarterly write completed")


# Run ETL
df_NAICS_raw, df_Monthly_raw, df_Quarterly_raw = extract()
df_NAICS = transform_NAICS(df_NAICS_raw)
df_Monthly = transform_Monthly_Vac(df_Monthly_raw)
df_Quarterly = transform_Quarterly_Vac(df_Quarterly_raw)
load(df_NAICS, df_Monthly, df_Quarterly)


job.commit()