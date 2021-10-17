import sys
import logging
from Reporter import Reporter
from config import create_config
from pyspark.sql import SparkSession

# Create SparkSession object
spark = SparkSession.builder.master('local').appName('app').getOrCreate()
sc = spark.sparkContext

# Initialize eod_dir object
eod_dir = "/Users/PycharmProjects/Guided_Capstone_Project/Python_Scripts_For_Automation/Ingested_CSV_Files/All_Ingested_CSV_Files/"


# Main Reporter Python script
def run_reporter_etl(config):

    trade_date = config.get("production", "processing_date")  # TODO: Need to create section for trade_date later on.
    reporter = Reporter(spark, config)
    try:
        reporter.report(spark, trade_date, eod_dir)
    except Exception as e:
        print(e)
    return


# Call above functions using arguments created and specified below
if __name__ == "__main__":
    # Obtain config file as input if available, otherwise create config file object
    print("System first input is {}".format(str(sys.argv[0])))
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
        print("System 2nd input is {}".format(str(sys.argv[1])))
    else:
        config_file = '../config/config.ini'
        print("Please specify config file.")

    # Get logging info
    logger = logging.getLogger(__name__)  # What is this? Identifier for logger.

    # Create config file
    my_config = create_config(config_file)

    # Obtain log file from config file
    log_file = my_config["paths"].get("log_file")

    # Write data to logfile
    logging.basicConfig(
        # TODO: Review components of logging file i.e. format.
        filename=log_file,
        filemode='w',
        format='%(asctime)s %(message)s',
        datefmt='%m%d%Y %I:%M:%S',
        level=logging.DEBUG
    )

    # StreamHandler object creation --> Used to send logging output to streams such as sys.stdout, sys.stderr.
    ch = logging.StreamHandler()

    # Set level for logging
    ch.setLevel(logging.INFO)

    # Call addHandler
    logger.addHandler(ch)

    # Call run_reporter_etl(my_config)
    run_reporter_etl(my_config)

    # Enter logging information.
    logger.info("Daily Reporting ETL Job complete!")
