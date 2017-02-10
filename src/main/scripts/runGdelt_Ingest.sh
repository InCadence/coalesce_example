#!/bin/bash
export GDELT_HOME=$PWD
java -cp 'lib/*' com.incadencecorp.coalesce.ingester.gdelt.GDELT_Ingester conf/gdelt.properties $1 