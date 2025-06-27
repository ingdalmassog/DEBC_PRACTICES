#! /bin/bash

wget -P /home/hadoop/temp_landing https://data-engineer-edvai-public.s3.amazonaws.com/2021-informe-ministerio.csv
wget -P /home/hadoop/temp_landing https://data-engineer-edvai-public.s3.amazonaws.com/202206-informe-ministerio.csv


# Check if wget command was successful
if [ $? -eq 0 ]; then
    echo "Download successful"
    # Delete old files in HDFS
     /home/hadoop/hadoop/bin/hdfs dfs -rm hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv
     /home/hadoop/hadoop/bin/hdfs dfs -rm hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv

    # Put the new files in HDFS
    /home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/temp_landing/2021-informe-ministerio.csv hdfs://172.17.0.2:9000/ingest/2021-informe-ministerio.csv
    /home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/temp_landing/202206-informe-ministerio.csv hdfs://172.17.0.2:9000/ingest/202206-informe-ministerio.csv

    if [ $? -eq 0 ]; then
        echo "File moved to Hadoop hdfs"

        # Remove temporal files
        rm -rf /home/hadoop/temp_landing/2021-informe-ministerio.csv
        rm -rf /home/hadoop/temp_landing/202206-informe-ministerio.csv

        echo "Cleaned temporal directory /home/hadoop/temp_landing/"
        exit 0
    else
        echo "Fail moving the file to Hadoop hdfs"
        exit 1
    fi
else
     echo "Download failed."
fi
