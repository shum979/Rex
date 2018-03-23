#!/bin/bash

usage ()
{
  echo 'Usage : Script -f <flow xml File name> '
  exit -1
}

while getopts ":f:" opt; do
  case $opt in
    f) FILE="$OPTARG"
    ;;
    \?) usage  >&2
    ;;
  esac
done


echo "Starting with Flow xml file:" "$FILE"

if [ $FILE != "" ]; then
    spark-submit --driver-class-path "file:///home/hadoop/conf/" --class rex.main.DataFlowAppDriver /home/hadoop/lib/accelerator-flowexecutor-1.0-SNAPSHOT-jar-with-dependencies.jar $FILE
else
    echo "Please provide flow xml file to start"
fi

