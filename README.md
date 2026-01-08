SmartCity project  simulates data from IoT device and produce those data into Kafka. The data from Kafka are processed through Spark and stored into Amazon S3 as parquet files
#### Docker Start
~~~
docker compose up -d
~~~
#### Python run
~~~
python jobs/main.py
~~~

#### Docker
~~~

 docker exec -it spark-master /opt/spark/bin/spark-submit \
--master spark://spark-master:7077 \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469 \
--conf spark.jars.ivy=/tmp/.ivy2 /opt/spark/jobs/smart_city.py


~~~
