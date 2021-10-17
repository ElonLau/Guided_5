import sys
import logging
from Tracker import Tracker
from config import create_config


def track_data_ingestion(config):

    tracker = Tracker("Data_Ingestion", config)
    job_id = tracker.assign_job_id()
    connection = tracker.get_db_connection()
    connection
    try:
        # In addition, create methods to assign job_id and get db connection.
        tracker.ingest_all_data()
        tracker.update_job_status("Successful Data Ingestion.", job_id, connection)
    except Exception as e:
        print(e)
        tracker.update_job_status("Failed Data Ingestion.", job_id, connection)
    return


if __name__ == "__main__":
    print("System first input is {}".format(str(sys.argv[0])))
    if len(sys.argv) > 1:
        config_file = sys.argv[1]
        print("System 2nd input is {}".format(str(sys.argv[1])))
    else:
        config_file = '../config/config.ini'
        print("Please specify config file.")

    # Tracker Data Ingestion and log info into log file.

    # Create my_config object to get info from config_file
    my_config = create_config(config_file)

    # Obtain log file from config file
    logger = logging.getLogger(__name__)

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

    # StreamHandler object creation
    ch = logging.StreamHandler()

    # Set level for logging
    ch.setLevel(logging.INFO)

    # Call addHandler
    logger.addHandler(ch)

    # Call run_reporter_etl(my_config)
    track_data_ingestion(my_config)

    # Enter logging information.
    logger.info("Daily Reporting ETL Job complete!")
