#!/bin/bash

GDELT_HOME=/opt/gdelt_ingest

java -cp "$GDELT_HOME/lib/*" com.incadencecorp.coalesce.ingester.gdelt.GDELT_Ingester $GDELT_HOME/conf/gdelt.properties $*

