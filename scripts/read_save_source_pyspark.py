from pyspark.sql import SparkSession
import argparse

usage='spark-submit ... this_python_file || This script is to fetch huge data from MySQL and save it as Parquet file in HDFS.'
desc = "Run it now.\n"+usage
parser = argparse.ArgumentParser(description=desc,
                                 epilog=usage,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)

parser.add_argument("-t", "--table", dest="table", type=str, help="table", required=True)
parser.add_argument("-d", "--db", dest="db", type=str, help="Database", required=True)
parser.add_argument("-p", "--path", dest="path", type=str, help="Output path", required=True)

args = parser.parse_args()

def fetch_mysql_credentials_from_conn_or_vault():
    # Logic goes here
    return {}

spark = SparkSession.builder \
    .appName("Students") \
    .getOrCreate()

mysql_credentials = fetch_mysql_credentials_from_conn_or_vault()
mysql_url = f"jdbc:mysql://{mysql_credentials.host}:{mysql_credentials.port}/{args.db}"

mysql_properties = {
    "user": mysql_credentials.user,
    "password": mysql_credentials.password,
    "driver": "com.mysql.cj.jdbc.Driver"
}

df = spark.read.jdbc(
    url=mysql_url,
    table="STUDENTS",
    properties=mysql_properties
).select("student_code", "honors_subject", "percentage_of_marks") # Select only allowed columns, can be passed from DAG


df.write.mode("overwrite").parquet(args.path, compression="snappy")
