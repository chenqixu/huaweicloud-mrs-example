#!/usr/bin/env bash

CLASSPATH=
for i in lib/*.jar; do CLASSPATH="$CLASSPATH":"$i"; done
CLASSPATH="$CLASSPATH":"."
#MAIN_CLASS="com.huawei.bigdata.hive.example.JDBCExample"
MAIN_CLASS="com.huawei.bigdata.hive.example.JDBCExamplePreLogin"
/opt/hwclient/JDK/jdk1.8.0_242/bin/java -cp ${CLASSPATH} ${MAIN_CLASS}