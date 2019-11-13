#!/usr/bin/env python
# coding: utf-8

# In[8]:


import pyspark 
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrameWriter as W
from pyspark.sql.types import StringType,FloatType,DoubleType
from pyspark.sql.window import Window
from pyspark.sql.functions import udf,col, countDistinct

MODULE_NAME='locator'
VERSION='0.01'

sc = SparkContext.getOrCreate()
spark = SparkSession(sc)

def get_module_name():
    return MODULE_NAME

def get_module_version():
    return VERSION
    

def get_dataset(file_location, file_type, infer_schema="false",                has_header="true", delimiter=","):
    
   df = spark.read.format(file_type)   .option("inferSchema", infer_schema)   .option("header", has_header)   .option("sep", delimiter)   .option("charset", "utf-8")   .load(file_location)

   return df

def get_country_info():
    
    file_location = "wikipedia-iso-country-codes.csv"
    file_type = "csv"
    
    df_country_info = get_dataset(file_location,        file_type, delimiter=",")
    df_country_info_renamed = df_country_info        .select(col("English short name lower case").alias("Country_Name")        ,col("Alpha-2 code").alias("ALPHA2"),col("Alpha-3 code").alias("ALPHA3"))
    
    return df_country_info_renamed

def filter_data(input_df,filter_key, filter_value):
    return input_df.filter(input_df[filter_key] == filter_value)

#Get Distance between 2 coordinates using Haversine Algorithm
def get_distance(lon_a, lat_a, lon_b, lat_b):
    from math import radians, cos, sin, asin, sqrt
    # Transform to radians
    lon_a, lat_a, lon_b, lat_b = map(radians, [lon_a, lat_a, lon_b, lat_b])    
    # Calculare distance between lon/lat
    dist_between_lon = lon_b - lon_a
    dist_between_lat = lat_b - lat_a  
    # Calculate area
    area = sin(dist_between_lat/2)**2 + cos(lat_a) * cos(lat_b)        * sin(dist_between_lon/2)**2  
    # Calculate the central angle
    central_angle = 2 * asin(sqrt(area))
    # Calculate Distance
    earth_radius = 6371
    distance_in_km = central_angle * earth_radius
    
    return abs(round(distance_in_km,1))

def string_to_float(x): 
    return float(x)

#User-Defined Functions Here
udf_string_to_float = udf(string_to_float, StringType())
udf_get_distance = F.udf(get_distance)

def get_isolated_stores_per_country(df_stores, country_name):

    df_stores = filter_data(df_stores, 'Country', country_name )
    #Use dataset with few columns/features for easy analysis
    df_stores_locations_only = df_stores        .select("STORE NUMBER","STORE NAME","LATITUDE","LONGITUDE")

    #Do a cross-join to perform column operations and renaming
    #This will pair each location to every location in the list
    df_stores_location_pairs = df_stores_locations_only        .crossJoin(df_stores_locations_only)        .toDF("STORE_NUMBER_A", "STORE_NAME_A", "LATITUDE_A", "LONGITUDE_A"            ,"STORE_NUMBER_B", "STORE_NAME_B", "LATITUDE_B", "LONGITUDE_B")
    
    #Clean up. Remove repeated rows
    df_stores_pairs = (df_stores_location_pairs
        .filter(df_stores_location_pairs.STORE_NUMBER_A 
        != df_stores_location_pairs.STORE_NUMBER_B))

    #Now get absolute distance between the pairs by calling the get_distance function
    df_store_pairs_with_distance = (df_stores_pairs        .withColumn("ABS_DISTANCE"        ,udf_get_distance(df_stores_pairs.LONGITUDE_Adf_stores_pairs.LATITUDE_A,        df_stores_pairs.LONGITUDE_B,df_stores_pairs.LATITUDE_B).cast(DoubleType())))
    
    #Sort Ascending
    df_store_pairs_with_distance = df_store_pairs_with_distance        .select("STORE_NUMBER_A","STORE_NAME_A","STORE_NUMBER_B","STORE_NAME_B"        ,"ABS_DISTANCE").orderBy("ABS_DISTANCE")
    
    '''
    - Apply min-max algorithm
    - First, sort by abs_distance in ascending order
    - Get the minimum nearest distance for each group
    - 
    '''
    #Group per store to its nearest_neighboor
    windowSpec = Window        .partitionBy(df_store_pairs_with_distance['STORE_NUMBER_A'])        .orderBy('ABS_DISTANCE')
        
    min_nearest_store_distance = F.first(df_store_pairs_with_distance['ABS_DISTANCE'])        .over(windowSpec)
    
    df_store_pairs_with_min_distance = df_store_pairs_with_distance        .select(df_store_pairs_with_distance['STORE_NUMBER_A']
        ,df_store_pairs_with_distance['STORE_NAME_A']
        ,df_store_pairs_with_distance['STORE_NUMBER_B']
        ,df_store_pairs_with_distance['STORE_NAME_B']
        ,df_store_pairs_with_distance['ABS_DISTANCE']
        ,min_nearest_store_distance.alias('min_nearest_distance'))

    #Generate dataset with minimum nearest neighboor distance.
    df_stores_isolated = df_store_pairs_with_min_distance        .filter(col("ABS_DISTANCE") == col("min_nearest_distance"))        .orderBy("min_nearest_distance",ascending=False)
    
    return df_stores_isolated.collect()
    
    
def get_most_isolated_store_per_country(df, country):
    out_df = get_isolated_stores_per_country(df, country)
    return {
        "store_name": out_df[0]['STORE_NAME_A'],
        "store_number": out_df[0]['STORE_NUMBER_A'],
        "nearest_store": out_df[0]['STORE_NAME_B'],
        "nearest_distance_km": out_df[0]['min_nearest_distance']
    }

def get_store_count_per_country(df):
    
    df_stores_agg = df.groupBy("Country_Name")        .agg(F.count("Store Number")        .alias('Store_Count'))        .orderBy(col("Store_Count"))
        
    df_country_info = get_country_info()
    
    df_stores_agg_joined = df_stores_agg.join(df_country_info        ,df_stores_agg.Country_Name == df_country_info.Country_Name,how='left')
    df_stores_agg_joined = df_stores_agg_joined.select(df_stores_agg['Country_Name']        ,df_stores_agg_joined['AlPHA2'],df_stores_agg_joined['ALPHA3']        ,df_stores_agg_joined['Store_Count'])
    
    #df_stores_agg_joined.show()
    
    return  df_stores_agg_joined

def get_country_store_count(df, country):
    out_df = get_store_count_per_country(df)
    out_df = out_df.filter(out_df.AlPHA2 == country ).collect()
    
    return out_df[0]['Store_Count']
    
def get_minimum_count(df, col_name):
    return df.select(F.min(col_name).alias('count')).collect()[0]

def get_countries_with_least_starbucks_stores(df):
    out_df = get_store_count_per_country(df)
    min_count = get_minimum_count(out_df, 'Store_Count')
    countries_with_least_stores_df = out_df.where(out_df.Store_Count == min_count['count'])
    
    return countries_with_least_stores_df.collect()

def merge_dataset(old_df, new_df, merge_key):
    df_joined = old_df.join(new_df,old_df[merge_key] == new_df[merge_key], how='left')
    return df_joined

def get_pair_wise_frequency(df, col_1, col_2, col2_filter ):
    df_pw_freq =  df.where((df[col_2] == col2_filter ))        .crosstab(col_1, col_2)
    return df_pw_freq
    


# In[ ]:




