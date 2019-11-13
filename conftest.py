from pyspark import SparkContext
from pyspark.sql import SQLContext
import pytest
import pyspark
from pyspark.sql.functions import udf,col, countDistinct
from pyspark.sql.types import StringType,FloatType,DoubleType

@pytest.fixture(scope='session')
def sql_context():
    spark_context = SparkContext()
    sql_context = SQLContext(spark_context)
    yield sql_context
    spark_context.stop()
