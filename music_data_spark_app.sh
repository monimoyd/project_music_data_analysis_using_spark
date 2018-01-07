#!/bin/bash

export HBASE_PATH=`/usr/local/hbase/bin/hbase classpath`
cd $SPARK_HOME

echo "Starting the Music Data Application"
bin/spark-submit --class final_project.MusicDataProcessorApp --driver-class-path $HBASE_PATH --master local[2] --packages com.databricks:spark-csv_2.10:1.4.0  ~/scala_eclipse/workspace/process-music-data/target/process_music_data-0.0.1-SNAPSHOT.jar > output.log 2>&1


echo "Completed the Music Data Application"
