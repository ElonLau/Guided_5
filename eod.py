from pyspark.sql import DataFrame, Window
from pyspark.sql.functions import col, max


class EODReport:

    def __init__(self, config):
        self._output_container = config.get('APP_CONFIG', 'OutputContainer')
        self._eod_report_dir = config.get('APP_CONFIG', 'EodReportDir')
        self._processing_date = config.get('PRODUCTION', 'ProcessingDate')  # str

    @staticmethod
    def apply_latest(df: DataFrame):
        """ Return quote or trade dataframe filtering out all but the most recent arrival times per key. """
        partition_key = Window.partitionBy("trade_dt", "symbol", "exchange", "event_tm", "event_seq_nb")
        max_column = "arrival_tm"
        return df.withColumn('tmp', max(max_column).over(partition_key)) \
            .filter(col('tmp') == col(max_column)) \
            .drop(col('tmp'))

    def run(self, common_df: DataFrame):
        """
        Apply transformations and write trade and quote reports to cloud storage.
        Args:
           common_df: combined quote and trade records, pre-filtered for the reporting day.
       """
        trade_columns = ["trade_dt", "symbol", "rec_type", "exchange", "event_tm", "event_seq_nb",
                         "arrival_tm", "trade_pr"]
        quote_columns = ["trade_dt", "symbol", "rec_type", "exchange", "event_tm", "event_seq_nb",
                         "arrival_tm", "bid_pr", "bid_size", "ask_pr", "ask_size"]

        trade_df = common_df.select(trade_columns).where(col('rec_type') == 'T').drop(col('rec_type'))
        quote_df = common_df.select(quote_columns).where(col('rec_type') == 'Q').drop(col('rec_type'))

        EODReport.apply_latest(trade_df)\
            .write.mode('overwrite')\
            .parquet(f"{self._output_container}/{self._eod_report_dir}/trade_dt={self._processing_date}")

        EODReport.apply_latest(quote_df)\
            .write.mode('overwrite') \
            .parquet(f"{self._output_container}/{self._eod_report_dir}/quote_dt={self._processing_date}")
