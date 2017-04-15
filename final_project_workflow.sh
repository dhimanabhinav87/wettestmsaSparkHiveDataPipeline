#!/bin/bash
set -e
echo "Copying files from Edge node to HDFS now"
sh /home/cloudera/FinalProject/copy_file.sh /home/cloudera/FinalProject/srcPrecipitation/*.txt /user/cloudera/sparkproject/landing/precipitation/ 
sh /home/cloudera/FinalProject/copy_file.sh /home/cloudera/FinalProject/srcCityStateMsa/*.txt /user/cloudera/sparkproject/landing/city_state_msa/ 
sh /home/cloudera/FinalProject/copy_file.sh /home/cloudera/FinalProject/srcPopMsa/*.csv /user/cloudera/sparkproject/landing/msa_pop/ 
#sh /home/cloudera/FinalProject/copy_file.sh /home/cloudera/FinalProject/param_conf /user/cloudera/sparkproject/conf/ 
echo "Copying files done"

echo "Staring spark-submit to process the files"
spark-submit --class geo.company.dept.WettestMsaYearMonth  --name "wettestMSAByYearMonth"     --master local     --num-executors 4     --executor-cores 4     --executor-memory 4g    --driver-memory 4g --driver-cores 1 /home/cloudera/FinalProject/wettestmsa.jar "hdfs://quickstart.cloudera:8020/user/cloudera/sparkproject/landing/conf" "/home/cloudera/FinalProject/station_landing/station.txt"
echo "spark action completed"

echo "Staring hive action to create tables"
hive -f /home/cloudera/FinalProject/hive_action_final
echo "Hive action completed"

echo "Staring cleanup"
hadoop fs -rm -r  /user/cloudera/sparkproject/landing/precipitation/*.txt 
hadoop fs -rm -r  /user/cloudera/sparkproject/landing/city_state_msa/*.txt 
hadoop fs -rm -r  /user/cloudera/sparkproject/landing/msa_pop/*.csv
echo "Cleanup done and workflow is completed"

