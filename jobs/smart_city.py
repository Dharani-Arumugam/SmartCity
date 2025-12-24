from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

from config import configuration



def main():
    spark = SparkSession.builder.appName("SmartCityStreaming") \
           .config("spark.jars.package",
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
                   "org.apache.hadoop:hadoop-aws:3.3.1,"
                   "com.amazonaws:aws-java-sdk:1.11.469") \
           .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
           .config("spark.hadoop.fs.s3a.access.key", configuration.get('AWS_ACCESS_KEY')) \
           .config("spark.hadoop.fs.s3a.secret.key", configuration.get('AWS_SECRET_KEY')) \
           .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
           .getOrCreate()
    # Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel('WARN')

    #vehicle schema
    vehicleSchema = StructType([
        StructField('id', StringType(), nullable=True),
        StructField('device_id', StringType(), nullable=True),
        StructField('timestamp', TimestampType(), nullable=True),
        StructField('location', StringType(), nullable=True),
        StructField('speed', DoubleType(), nullable=True),
        StructField('direction', StringType(), nullable=True),
        StructField('make', StringType(), nullable=True),
        StructField('model', StringType(), nullable=True),
        StructField('year', IntegerType(), nullable=True),
        StructField('fuel_type', StringType(), nullable=True),
        ])

    #gps schema
    gpsSchema = StructType([
        StructField('id', StringType(), nullable=True),
        StructField('device_id', StringType(), nullable=True),
        StructField('timestamp', TimestampType(), nullable=True),
        StructField('speed', DoubleType(), nullable=True),
        StructField('direction', StringType(), nullable=True),
        StructField('vehicle_type', StringType(), nullable=True),
    ])

    #traffic camera data
    trafficCameraSchema = StructType([
        StructField('id', StringType(), nullable=True),
        StructField('device_id', StringType(), nullable=True),
        StructField('timestamp', TimestampType(), nullable=True),
        StructField('location', StringType(), nullable=True),
        StructField('cameraId', StringType(), nullable=True),
        StructField('snapshot', StringType(), nullable=True),
    ])

    #weather data
    weatherSchema = StructType([
        StructField('id', StringType(), nullable=True),
        StructField('deviceId', StringType(), nullable=True),
        StructField('timestamp', TimestampType(), nullable=True),
        StructField('location', StringType(), nullable=True),
        StructField('temperature', DoubleType(), nullable=True),
        StructField('weatherCondition', StringType(), nullable=True),
        StructField('precipitation', DoubleType(), nullable=True),
        StructField('windSpeed', DoubleType(), nullable=True),
        StructField('humidity', IntegerType(), nullable=True),
        StructField('airQualityIndex', DoubleType(), nullable=True),
    ])

    #emergency data
    emergencySchema = StructType([
        StructField('id', StringType(), nullable=True),
        StructField('deviceId', StringType(), nullable=True),
        StructField('incidentId', StringType(), nullable=True),
        StructField('incidentType', StringType(), nullable=True),
        StructField('timestamp', TimestampType(), nullable=True),
        StructField('location', StringType(), nullable=True),
        StructField('incidentStatus', StringType(), nullable=True),
        StructField('description', StringType(), nullable=True),
    ])

    def read_kafka_topic(topic, schema):
        return (spark.readStream
               .format("kafka")
               .option("kafka.bootstrap.servers", "broker:29092")
               .option("subscribe", topic)
               .option("startingOffsets", "earliest")
               .option("failOnDataLoss", "false")
               .load()
               .selectExpr('CAST(value AS STRING)')
               .select(from_json(col('value'), schema).alias('data'))
               .select('data.*')
               .withWatermark('timestamp', '2 minutes')
                )
    def streamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
                .format('parquet')
                .option("checkpointLocation", checkpointFolder)
                .option("path", output)
                .outputMode('append')
                .start())

    #read from Kafka
    vehicleDF           = read_kafka_topic('vehicle_data', vehicleSchema)
    gpsDF               = read_kafka_topic('gps_data', gpsSchema)
    trafficCameraDF     = read_kafka_topic('traffic_data', trafficCameraSchema)
    weatherDF           = read_kafka_topic('weather_data', weatherSchema)
    emergencyDF         = read_kafka_topic('emergency_data', emergencySchema)

    #join all the dataframes
    query1 = streamWriter(vehicleDF, 's3a://smartcity-dharani-de/checkpoints/vehicle_data',
                          's3a://smartcity-dharani-de/data/vehicle_data')

    query2 = streamWriter(gpsDF, 's3a://smartcity-dharani-de/checkpoints/gps_data',
                          's3a://smartcity-dharani-de/data/gps_data')

    query3 = streamWriter(trafficCameraDF, 's3a://smartcity-dharani-de/checkpoints/traffic_camera_data',
                          's3a://smartcity-dharani-de/data/traffic_camera_data')

    query4 = streamWriter(weatherDF, 's3a://smartcity-dharani-de/checkpoints/weather_data',
                          's3a://smartcity-dharani-de/data/weather_data')

    query5 = streamWriter(emergencyDF, 's3a://smartcity-dharani-de/checkpoints/emergency_data',
                          's3a://smartcity-dharani-de/data/emergency_data')

    query5.awaitTermination()

if __name__ == '__main__':
    main()