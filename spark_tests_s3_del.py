##
import json
from datetime import datetime
import pytz
import math.random
import sys
 
#def config 
config = """
{
    "src_bucket": "s3://sourcetestBucket/source/tables/all",
    "tgt_bucket": "s3://targettestBucket/target/tables/all",
    "years": [
        "20xx",
        "2023",
        "2015",
        "2008"
    ],
    "identifier_num": {
        "xxx123": ["456", "1-31-2019"],
        "xxx124": ["2", "1-1-2023"],
        "xxx125": ["c3", "1-11-2015"],
        "xxx678": ["3b", "1-1-2008"]
    }, 
    "main_table": "xdecopx",
    "tables_list": [ 
        "test1",
        "test2",
        "test3",
        "test4",
        "test5",
        "test6",
        "test7" 
    ],
    "id_num_end_date": "12-31-2023"
}
"""
 
# Extracting information from configurations and config file for DLL
config_js = json.loads(config)
source_bucket = config_js["src_bucket"]
target_bucket = config_js["tgt_bucket"]
years_list = config_js["years"]
id_num_dict = config_js["identifier_num"]
main_table = config_js["main_table"]
tables_list = config_js["tables_list"]
id_num_end_date = config_js["id_num_end_date"]
end_date = datetime.strptime(id_num_end_date, "%m-%d-%Y").date()
 
# Writing for Root Base Table Entry
print("===================================This BLOCK IS LOADDING..==========================================================")
print(f"The block is for table : {main_table}.")
# Declaring counter variable
count = 0
print(f" The Current Date and Time : {datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d:%H:%M:%S')}")
 
for year in years_list:
    print("====================")
    print(f"Processing for year : {year}.")
 
    for id_num, id_num_value in id_num_dict.items():
        aID_num_value, start_date = id_num_value
 
        if datetime.strptime(start_date, "%m-%d-%Y").year <= datetime.strptime(year, "%Y").year:
            try:
                # Reading root table data
                main_df = spark.read.format("parquet").load(f"{source_bucket}/{main_table}/year={year}/")
                main_df.createOrReplaceTempView(f"{main_table}")
 
                sqldf4 = spark.sql(f"""SELECT * FROM {main_table} WHERE (id_num = '{id_num}' OR aID_num = '{aID_num_value}') AND to_date(d_ocur,'yyyy-MM-dd') >= '{datetime.strptime(f"{start_date}", "%m-%d-%Y").date()}' AND to_date(d_ocur,'yyyy-MM-dd') <= '{end_date}';""")
 
                count += sqldf4.count()
                sqldf4.write.parquet(f"{target_bucket}/{main_table}/year={year}/", mode="append")
            except Exception as e:
                print(f"Error encountered while writing for base table.")
                print(f"Error:- {e}")
                
print(f"Number of files of the source table {main_table} : {count}")
print(f"""Number of files of the destination table {main_table} : {spark.read.format("parquet").load(f"{target_bucket}/{main_table}/").count()}""")
print("====================================================================================================")
 
# Writing for Remaining Child Node Tables
for table in tables_list:
    print(f"The block is for table : {table}.")
    # Declaring counter variable
    count = 0
    print(f"The Current Date and Time : {datetime.now(pytz.timezone('America/New_York')).strftime('%Y-%m-%d:%H:%M:%S')}")
 
    for year in years_list:
        print("====================")
        print(f"Processing for some years i.e. : {year}.")
        try:
            for id_num, id_num_value in id_num_dict.items():
                aID_num_value, start_date = id_num_value
 
                if datetime.strptime(start_date, "%m-%d-%Y").year <= datetime.strptime(year, "%Y").year:
                    # Reading root table data
                    main_df = spark.read.format("parquet").load(f"{source_bucket}/{main_table}/year={year}/")
                    main_df.createOrReplaceTempView(f"{main_table}")
        
                    table_df = spark.read.format("parquet").load(f"{source_bucket}/{table}/year={year}/")
                    table_df.createOrReplaceTempView("table_df")
    
                    sqldf1 = spark.sql(f"""SELECT * FROM {main_table} WHERE (id_num = '{id_num}' OR aID_num = '{aID_num_value}') AND (to_date(d_ocur,'yyyy-MM-dd') >= '{datetime.strptime(f"{start_date}", "%m-%d-%Y").date()}' AND to_date(d_ocur,'yyyy-MM-dd') <= '{end_date}');""")
    
                    if sqldf1.count() != 0:
                        sqldf1.createOrReplaceTempView("main_table1")
                        sqldf4 = spark.sql(f"select t1.* from table_df t1 inner join main_table1 t2 on (t1.id_num_cl = t2.id_num_cl and t1.date_insert = t2.date_insert)")
    
                        count += sqldf4.count()
                        sqldf4.write.parquet(f"{target_bucket}/{table}/year={year}/", mode="append")
        except Exception as e:
            print(f"Error encountered while writing for {table} table.")
            print(f"Error:- {e}")
 
    print(f"Number of records of the source table {table} : {count}")
    print(f"""Number of records of the destination table {table} : {spark.read.format("parquet").load(f"{target_bucket}/{table}/").count()}""")
    print("====================================================================================================")
print("============================The PROCESS has COMPLETED!===================================================")

#####deleting a dir from s3 in aws

##<<>>#####

# import boto3
# import datetime
# import argparse
# # s3://va-testBucket/Planting101/2021

# if __name__ == "main":
#     #defining the cmd args
#     parser = argparse.ArgumentParser()
#     parser.add_argument('2008-01-01', type=str, required=True, help="Start date in the format YYYY-MM-DD")
#     parser.add_argument('2023-07-04', type=str, required=True, help='End date in the format YYYY-MM-DD')
#     parser.add_argument('s3://va-TestBucket-Non-prod/2023/', type=str, required=True, help='S3 bucket in AWS')
#     arg = parser.parser_args()

#     start_Date = datetime.date.strptime(args,start_Date, '%Y-%m-%d')
#     end_Date = datetime.date.strptime(args, end_Date, '%Y-%m-%d')

#     s3 = boto3.client('s3')
#     s3_objects = s3.list_objects_v2(Bucket=args.bucket_name)

#     keys_to_delete = []

#     for obj in objects['Contents']:
#         parts = obj['Key'].split('/')
#         insert_Date = None
#         for part in parts:
#             if "Date_Insert" in part:
#                 insert_Date = part.split('=')[1]
#         if not insert_Date:
#             continue
#         if '-' in insert_Date:
#             date = datetime.datetime.strptime(insert_Date, '%Y-%m-%d')
#         else:
#             date = datetime.datetime.strptime(insert_Date, '%Y%m%d')
#         if start_Date <= date <= end_Date:
#             keys_to_delete.append({'Key': obj['Key']})
#             if len(keys_to_delete) > 800:
#                 s3.delete_objects(Bucket = args.bucket_name, Delete = {'Objects' : keys_to_delete})
#                 keys_to_delete = []

#     if len(keys_to_delete) > 0:
#         s3.delete_objects(Bucket = args.bucket_name, Delete = {'Objects': keys_to_delete})




