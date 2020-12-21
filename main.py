import time
import pyspark.sql as ps
from pyspark.sql.functions import *
from pyspark.sql.types import *

class MLDetection:

    def __init__ (self, data_path, date_range, fee_range):
        self.data_path = data_path
        self.date_range = date_range
        self.fee_range = fee_range

        # initiate spark session
        self.spark = ps.SparkSession \
            .builder \
            .master("local[*]") \
            .getOrCreate()

    def detection(self):
        # read raw data into dataframe, with pre defined columns and data types
        schema = ps.types.StructType() \
              .add("TRANSACTION", StringType(), True) \
              .add("TIMESTAMP", StringType(), True) \
              .add("AMOUNT", FloatType(), True) \
              .add("SENDER", StringType(), True) \
              .add("RECEIVER", StringType(), True)

        self.df_raw = self.spark.read \
            .option("header", True) \
            .option("delimiter", "|") \
            .schema(schema) \
            .csv(self.data_path)

        # preprocess on timestamp
        self.df_raw = self.df_raw.withColumn('TIMESTAMP', \
                    when(self.df_raw.TIMESTAMP != '2006-02-29', self.df_raw.TIMESTAMP) \
                    .otherwise('2006-02-28'))
        self.df_raw = self.df_raw.withColumn('TIMESTAMP', self.df_raw.TIMESTAMP.cast(DateType()))

        # preprocess on sender and receiver
        self.df_raw = self.df_raw.filter(self.df_raw.SENDER != self.df_raw.RECEIVER)

        # crossjoin and detection
        df_re = self.df_raw.select([col(c).alias(c+"_re") for c in self.df_raw.columns])
        df_se = self.df_raw.select([col(c).alias(c+"_se") for c in self.df_raw.columns])

        self.df_join = df_re.crossJoin(df_se) \
                        .drop('SENDER_re', 'RECEIVER_se') \
                        .filter('TRANSACTION_re != TRANSACTION_se') \
                        .filter('RECEIVER_re == SENDER_se') \
                        .filter('TIMESTAMP_se >= date_add(TIMESTAMP_re, {})'.format(self.date_range[0])) \
                        .filter('TIMESTAMP_se <= date_add(TIMESTAMP_re, {})'.format(self.date_range[1])) \
                        .filter('AMOUNT_se <= (1-{}) * AMOUNT_re'.format(self.fee_range[0])) \
                        .filter('AMOUNT_se >= (1-{}) * AMOUNT_re'.format(self.fee_range[1]))

        self.ml_trans = self.df_join.select('TRANSACTION_re').distinct() \
                        .union(self.df_join.select('TRANSACTION_se').distinct()) \
                        .distinct()

    def ouput(self):
        # get dataframe on ml trans
        self.df_ml_trans = self.ml_trans.join(self.df_raw, self.ml_trans.TRANSACTION_re == self.df_raw.TRANSACTION, 'inner') \
                .select('TRANSACTION', 'TIMESTAMP', 'AMOUNT', 'SENDER', 'RECEIVER').cache()

        # output suspicious transactions file
        print('Writing suspicious transactions...')
        self.df_ml_trans.coalesce(1) \
                    .write.format('csv') \
                    .option('header', True) \
                    .mode('overwrite') \
                    .save('suspicious_transactions')

        # output suspicious entities file
        entity_re = self.df_ml_trans.groupBy('RECEIVER').count().withColumnRenamed('count', 'R_COUNT')
        entity_se = self.df_ml_trans.groupBy('SENDER').count().withColumnRenamed('count', 'S_COUNT')

        self.df_ml_entities = entity_re.join(entity_se, entity_re.RECEIVER == entity_se.SENDER, 'outer') \
                                    .withColumn('Total_Count', \
                                    when(col('R_COUNT').isNull(), col('S_COUNT')) \
                                    .when(col('S_COUNT').isNull(), col('R_COUNT')) \
                                    .otherwise(col('S_COUNT') +  col('R_COUNT'))) \
                                    .withColumn('ENTITY', \
                                    when(col('RECEIVER').isNull(), col('SENDER')) \
                                    .when(col('SENDER').isNull(), col('RECEIVER')) \
                                    .otherwise(col('RECEIVER'))) \
                                    .select('ENTITY', 'Total_Count') \
                                    .sort('Total_Count', ascending=False)

        print('Writing suspicious entities...')
        self.df_ml_entities.coalesce(1) \
                    .write.format('csv') \
                    .option('header', True) \
                    .mode('overwrite') \
                    .save('suspicious_entities')

        # close the spark session
        self.spark.catalog.clearCache()
        self.spark.stop()


if __name__ == '__main__':

    t0 = time.time()

    # define inputs
    input_data = "transactions.csv"
    date_range = (0, 7)
    fee_range = (0.05, 0.25)

    # initiate the class
    print('Initiating solver class based on pyspark...')
    MLD = MLDetection(input_data, date_range, fee_range)

    # ML detection
    print('Begin ML trans detection...')
    MLD.detection()

    # Output results
    print('Output the results...')
    MLD.ouput()

    # computing time
    t1 = time.time()
    print('Completed in {} seconds.'.format(int(t1 - t0)))
