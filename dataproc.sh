#!/bin/bash
/home/cdsw/google-cloud-sdk/bin/gcloud dataproc clusters create sgupta --bucket=sgupta_doubleclick --master-boot-disk-size=10GB --master-machine-type=n1-standard-2 --num-masters=1 --worker-boot-disk-size=10GB --worker-machine-type=n1-standard-2
/home/cdsw/google-cloud-sdk/bin/gcloud dataproc jobs submit pyspark --cluster sgupta TransformData_spark.py
/home/cdsw/google-cloud-sdk/bin/gcloud dataproc clusters delete sgupta --quiet

DATE=`date +%Y-%m-%d`

for i in $( /home/cdsw/google-cloud-sdk/bin/gsutil ls gs://sgupta_doubleclick/impressions/*/date=$DATE/*|grep parquet ); do
  echo Loading file $i into BigQuery DoubleClick.impressions dataset
  /home/cdsw/google-cloud-sdk/bin/bq load --source_format=PARQUET DoubleClick.impressions $i
done
