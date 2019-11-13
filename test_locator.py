from qantas import locator
import pandas as pd
import pyspark
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import udf,col, countDistinct
from pyspark.sql.types import StringType,FloatType,DoubleType

file_location = "starbucks_store_locator.csv"
file_type = "csv"
df_starbucks_stores = locator.get_dataset(file_location, file_type, delimiter=",")
df_starbucks_stores = df_starbucks_stores.filter((col('Longitude').isNotNull()) | (col('Latitude').isNotNull()))
df_starbucks_stores = df_starbucks_stores\
                      .withColumn("Longitude", df_starbucks_stores["Longitude"].cast(FloatType()))\
                      .withColumn("Latitude", df_starbucks_stores["Latitude"].cast(FloatType()))
df_country_info = locator.get_country_info()
df_starbucks_country_joined = df_starbucks_stores.join(df_country_info,df_starbucks_stores.Country == df_country_info.ALPHA2,how='left')
df_starbucks_stores_with_country_name = df_starbucks_country_joined.filter(col('ALPHA2').isNotNull())

def test_get_module_name():
    assert locator.get_module_name() == 'locator',"test failed"

def test_get_module_version():
	assert locator.get_module_version() == '0.01',"test failed"


def test_get_dataset():
    file_location = "starbucks_store_locator.csv"
    file_type = "csv"
    df =  locator.get_dataset(file_location, file_type, delimiter=",")
    res_count = len (df.columns)
    assert res_count == 13, "test failed"


def test_get_country_info():
    df = locator.get_country_info()
    cols = df.columns
    assert cols[0] == 'Country_Name',"test failed"
    assert cols[1] == 'ALPHA2',"test failed"
    assert cols[2] == 'ALPHA3',"test failed"


def test_get_distance():
    lat_a = -5.14
    lon_a = 119.42 
    lat_b = -1.26
    lon_b = 116.9

    assert locator.get_distance(lon_a, lat_a, lon_b, lat_b) == 514.2, "test failed"


def test_get_most_isolated_store():

    file_location = "starbucks_store_locator.csv"
    file_type = "csv"
    df_starbucks_stores = locator.get_dataset(file_location, file_type, delimiter=",")
    df_starbucks_stores = df_starbucks_stores.filter((col('Longitude').isNotNull()) | (col('Latitude').isNotNull()))
    df_starbucks_stores = df_starbucks_stores\
                      .withColumn("Longitude", df_starbucks_stores["Longitude"].cast(FloatType()))\
                      .withColumn("Latitude", df_starbucks_stores["Latitude"].cast(FloatType()))
    df_country_info = locator.get_country_info()
    df_starbucks_country_joined = df_starbucks_stores.join(df_country_info,df_starbucks_stores.Country == df_country_info.ALPHA2,how='left')
    df_starbucks_stores_with_country_name = df_starbucks_country_joined.filter(col('ALPHA2').isNotNull())
  
    #isolated = get_most_isolated_store_per_country(df_starbucks_stores_with_country_name,'AU') 
    #assert isolated['store_name'] == 'Mount Druitt', "test failed"
   

def test_get_countries_with_least_starbucks_stores():
    least_stores = locator.get_countries_with_least_starbucks_stores(df_starbucks_stores_with_country_name)
    assert least_stores[0]["Country_Name"] == 'Andorra', "test failed"
    assert least_stores[0]["Store_Count"] == 1, "test failed"
