#! /bin/bash

# su hadoop

wget -P /home/hadoop/temp_landing https://data-engineer-edvai-public.s3.amazonaws.com/aeropuertos_detalle.csv

# Check if wget command was successful
if [ $? -eq 0 ]; then
    echo "Download successful"
    # hdfs dfs -put /home/hadoop/temp_landing/aeropuertos_detalle.csv ingest/flights_data
    /home/hadoop/hadoop/bin/hdfs dfs -rm hdfs://172.17.0.2:9000/ingest/aeropuertos_detalle.csv
    # /home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/temp_landing/aeropuertos_detalle.csv ingest/
    /home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/temp_landing/aeropuertos_detalle.csv hdfs://172.17.0.2:9000/ingest/
    # hadoop dfs -put /home/hadoop/temp_landing/aeropuertos_detalle.csv ingest/flights_data
    if [ $? -eq 0 ]; then
        echo "File moved to Hadoop hdfs"
        rm -rf /home/hadoop/temp_landing/aeropuertos_detalle.csv
        echo "Cleaned temporal directory /home/hadoop/temp_landing/"
        exit 0
    else
        echo "Fail moving the file to Hadoop hdfs"
        exit 1
    fi
else
    echo "Download failed."
fi
