#!/bin/bash

cd $SPARK_HOME

export HBASE_PATH=`/usr/local/hbase/bin/hbase classpath`

echo "Loading StndIdGeocd table in HBase"
bin/spark-submit --class final_project.LoadHBaseTables --driver-class-path $HBASE_PATH  --master local[2] ~/scala_eclipse/workspace/load-hbase-tables/target/load_hbase_tables-0.0.1-SNAPSHOT.jar /home/acadgild/final_project/stn-geocd.txt StndIdGeoCd Geo_cd,name
echo "Loaded StndIdGeocd table in HBase"

echo "Loading UserArtist table in HBase"
bin/spark-submit --class final_project.LoadHBaseTables --driver-class-path $HBASE_PATH  --master local[2] ~/scala_eclipse/workspace/load-hbase-tables/target/load_hbase_tables-0.0.1-SNAPSHOT.jar /home/acadgild/final_project/user-artist.txt UserArtist Artist,id
echo "Loaded UserArtist table in HBase"

echo "Loading SongArtist table in HBase"
bin/spark-submit --class final_project.LoadHBaseTables --driver-class-path $HBASE_PATH  --master local[2] ~/scala_eclipse/workspace/load-hbase-tables/target/load_hbase_tables-0.0.1-SNAPSHOT.jar /home/acadgild/final_project/song-artist.txt SongArtist Artist,id
echo "Loaded SongArtist table in HBase"

echo "Loading UserSubscription table in HBase"
bin/spark-submit --class final_project.LoadHBaseTables --driver-class-path $HBASE_PATH  --master local[2] ~/scala_eclipse/workspace/load-hbase-tables/target/load_hbase_tables-0.0.1-SNAPSHOT.jar /home/acadgild/final_project/user-subscn.txt UserSubscription Subscription,Start_ts,Subscription,End_ts
echo "Loaded UserSubscription table in HBase"

