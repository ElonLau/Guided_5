import json
import logging
from datetime import datetime
from random import randint
import mysql.connector
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType


# Create Tracker class -> Do we inherit object (i.e. Tracker(object)?)
class Tracker:

    def __init__(self, jobname, dbconfig):
        self.jobname = jobname
        self.dbconfig = dbconfig

    def ingest_all_data(self):


        # Create SparkSession object within method
        spark = SparkSession.builder.master('local').appName('app').getOrCreate()
        sc = spark.sparkContext

        # Create inner function to be called inside function
        def parse_csv(line):
            fields = line.split(',')
            try:
                if fields[2] == 'Q':
                    trade_dt = fields[0]
                    rec_type = fields[2]
                    symbol = fields[3]
                    exchange = fields[6]
                    event_tm = fields[1]
                    event_seq_nb = fields[5]
                    arrival_tm = fields[4]
                    trade_pr = '-'
                    bid_pr = fields[7]
                    bid_size = fields[8]
                    ask_pr = fields[9]
                    ask_size = fields[10]
                    execution_id = 'NA'
                    trade_size = 'NA'
                    partition = "Q"
                elif fields[2] == 'T':
                    trade_dt = fields[0]
                    rec_type = fields[2]
                    symbol = fields[3]
                    exchange = fields[6]
                    event_tm = fields[1]
                    event_seq_nb = fields[5]
                    arrival_tm = fields[4]
                    trade_pr = fields[7]
                    bid_pr = '-'
                    bid_size = '-'
                    ask_pr = '-'
                    ask_size = '-'
                    execution_id = 'NA'
                    trade_size = 'NA'
                    partition = "T"
                # Return event as list
                event = [trade_dt, rec_type, symbol, exchange, event_tm, event_seq_nb, arrival_tm,
                         trade_pr, bid_pr, bid_size, ask_pr, ask_size, execution_id, trade_size, partition]
                return event
            # Rejected events
            except Exception as e:
                event = [None, None, None, None, None, None, None, None, None, None, None, None, None, None, "B"]
                logging.error("Bad record", e)
                return event

        def parse_json(line):
            fields = json.loads(line)
            try:
                if fields['event_type'] == 'Q':
                    trade_dt = fields['trade_dt']
                    rec_type = fields['event_type']
                    symbol = fields['symbol']
                    exchange = fields['exchange']
                    event_tm = fields['event_tm']
                    event_seq_nb = fields['event_seq_nb']
                    arrival_tm = fields['file_tm']
                    trade_pr = '-'
                    bid_pr = fields['bid_pr']
                    bid_size = fields['bid_size']
                    ask_pr = fields['ask_pr']
                    ask_size = fields['ask_size']
                    execution_id = '-'
                    trade_size = '-'
                    partition = "Q"
                elif fields['event_type'] == 'T':
                    trade_dt = fields['trade_dt']
                    rec_type = fields['event_type']
                    symbol = fields['symbol']
                    exchange = fields['exchange']
                    event_tm = fields['event_tm']
                    event_seq_nb = fields['event_seq_nb']
                    arrival_tm = fields['file_tm']
                    trade_pr = fields['price']
                    bid_pr = '-'
                    bid_size = '-'
                    ask_pr = '-'
                    ask_size = '-'
                    execution_id = fields['execution_id']
                    trade_size = fields['size']
                    partition = "T"
                # Return event object as list
                event = [trade_dt, rec_type, symbol, exchange, event_tm, event_seq_nb, arrival_tm,
                         trade_pr, bid_pr, bid_size, ask_pr, ask_size, execution_id, trade_size, partition]
                return event
            except Exception as e:
                event = [None, None, None, None, None, None, None, None, None, None, None, None, None, None, "B"]
                logging.error("Bad record", e)
                return event

        # Define common event schema
        common_event_schema = StructType([
            StructField("trade_dt", StringType(), True),
            StructField("rec_type", StringType(), True),
            StructField("symbol", StringType(), True),
            StructField("exchange", StringType(), True),
            StructField("event_tm", StringType(), True),
            StructField("event_seq_nb", StringType(), True),
            StructField("arrival_tm", StringType(), True),
            StructField("trade_pr", StringType(), True),
            StructField("bid_pr", StringType(), True),
            StructField("bid_size", StringType(), True),
            StructField("ask_pr", StringType(), True),
            StructField("ask_size", StringType(), True),
            StructField("execution_id", StringType(), True),
            StructField("trade_size", StringType(), True),
            StructField("partition", StringType(), True)  # THIS COLUMN IS USED FOR PARTITIONING ACROSS FILE SYSTEM
        ])


        # TODO: Try ".append" mode.

        # Parse CSV files and then write to folder as parquet file partitioned by "partition" column:
        raw_csv1 = sc.textFile(
            "/Users/PycharmProjects/Guided_Capstone_Project/Python_Scripts_For_Automation/GC_CSV_Files/part-00000-5e4ced0a-66e2-442a-b020-347d0df4df8f-c000.txt")
        parsed_csv1 = raw_csv1.map(lambda line: parse_csv(line))
        spark_csv_df1 = spark.createDataFrame(parsed_csv1, common_event_schema)
        spark_csv_df1.write.partitionBy('partition').mode('overwrite').parquet(ingest_csv_subfolder_1)

        raw_csv2 = sc.textFile(
            "/Users/PycharmProjects/Guided_Capstone_Project/Python_Scripts_For_Automation/GC_CSV_Files/part-00000-214fff0a-f408-466c-bb15-095cd8b648dc-c000.txt")
        parsed_csv2 = raw_csv2.map(lambda line: parse_csv(line))
        spark_csv_df2 = spark.createDataFrame(parsed_csv2, common_event_schema)
        spark_csv_df2.write.partitionBy('partition').mode('overwrite').parquet(ingest_csv_subfolder_2)

        # Use JSON Parser in Spark Transformation to Process the Source Data
        raw_json1 = sc.textFile(
            "/Users/PycharmProjects/Guided_Capstone_Project/Python_Scripts_For_Automation/GC_JSON_Files/part-00000-c6c48831-3d45-4887-ba5f-82060885fc6c-c000.txt")
        parsed_json1 = raw_json1.map(lambda line: parse_json(line))
        spark_json_df1 = spark.createDataFrame(parsed_json1, common_event_schema)
        spark_json_df1.write.partitionBy('partition').mode('overwrite').parquet(ingest_json_subfolder_1)

        raw_json2 = sc.textFile(
            "/Users/PycharmProjects/Guided_Capstone_Project/Python_Scripts_For_Automation/GC_JSON_Files/part-00000-092ec1db-39ab-4079-9580-f7c7b516a283-c000.txt")
        parsed_json2 = raw_json2.map(lambda line: parse_json(line))
        spark_json_df2 = spark.createDataFrame(parsed_json2, common_event_schema)
        spark_json_df2.write.partitionBy('partition').mode('overwrite').parquet(ingest_json_subfolder_2)

        # Stop Spark Cluster
        sc.stop()

    def assign_job_id(self):


        num_string = str(randint(0, 10000)).zfill(5)
        job_id = self.jobname + str(num_string) + datetime.today().strftime("%Y%m%d")
        return job_id

    def update_job_status(self, status, assigned_job_id, connection_obj):


        job_id = assigned_job_id
        print("Job ID Assigned: {}".format(job_id))
        update_time = datetime.now()

        try:
            dbCursor = connection_obj.cursor()
            job_sql_command = "INSERT INTO job_tracker_table(job_id, job_status, update_time) " \
                              "VALUES('" + job_id + "', '" + status + "', '" + str(update_time) + "')"
            dbCursor.execute(job_sql_command)
            dbCursor.close()
            print("Inserted data into job tracker table.")
        except (Exception, mysql.connector.Error) as error:
            return logging.error("Error executing db statement for job tracker.", error)

    def get_db_connection(self):

        connection = None
        try:
            connection = mysql.connector.connect(user=self.dbconfig.get("mysql", "user"),
                                                 password=self.dbconfig.get("mysql", "password"),
                                                 host=self.dbconfig.get("mysql", "host"),
                                                 port=self.dbconfig.get("mysql", "port"),
                                                 database=self.dbconfig.get("mysql", "database"))
            return connection
        except Exception as error:
            logging.error("Error while connecting to database for job tracker", error)
