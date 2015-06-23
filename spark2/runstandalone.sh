#!/bin/bash

#$SPARK_HOME/sbin/start-all.sh
#spark-submit --master spark://namenode:7077 --class $1 ./recsys-1.0-SNAPSHOT.jar --executor-memory 6G

spark-submit \
        --master spark://namenode:7077 \
        --class $1 \
		--executor-memory 6G \
        $2
