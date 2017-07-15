#!/bin/sh
java -Dlog4j.configuration=file:conf/log4j.properties -cp target/gdelt_ingester-0.0.1-SNAPSHOT.jar  com/incadencecorp/coalesce/ingester/gdelt/GDELT_Ingester $*
