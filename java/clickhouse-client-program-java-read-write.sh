#!/bin/sh

#This programs runs the java program to write data to clickhouse

java -classpath lib/clickhouse-jdbc-0.3.2-patch7-shaded.jar  src/main/java/clickhouse-client/ReadWriteExample.java
