#!/bin/bash
#chmod 755
#

#--conf spark.sql.shuffle.partitions=300 --conf spark.default.parallelism=300
#export HADOOP_USER_NAME=ec2-user

NUM_EXEC=43
NUM_EXEC_CORE=5
NUM_EXEC_MEM=16g
DRIVER_CORES=5
DRIVER_MEMORY=16g
FILES=../conf/log4j.properties#log4j.properties
CONF=spark.driver.extraJavaOptions="-Dlog4j.configuration=file:../conf/log4j.properties"
CLASS=it.au.misure.cli.FMTool
JAR=fm-2.0.jar
JDBC_DRIVER=ojdbc7-12.7.0.jar


spark-submit \
--driver-class-path $JDBC_DRIVER \
--num-executors $NUM_EXEC \
--executor-cores $NUM_EXEC_CORE \
--executor-memory $NUM_EXEC_MEM \
--driver-cores $DRIVER_CORES \
--driver-memory $DRIVER_MEMORY \
--jars $JDBC_DRIVER \
--conf $CONF \
--files $FILES \
--class $CLASS \
$JAR $* 
