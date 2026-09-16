docker exec spark-master /opt/spark/bin/spark-submit `
  --master spark://spark-master:7077 `
  --name kafka-to-parquet-stream `
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 `
  --conf spark.jars.ivy=/tmp/.ivy2 `
  --conf spark.executor.cores=1 `
  --conf spark.cores.max=1 `
  --conf spark.executor.memory=512m `
  --conf spark.driver.memory=512m `
  --conf spark.sql.shuffle.partitions=2 `
  --conf spark.sql.files.maxPartitionBytes=32m `
  /opt/spark-apps/kafka_stream.py