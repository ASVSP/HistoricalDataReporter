docker exec -it namenode bash -c "chmod +x ./asvsp/data/load-data.sh && ./asvsp/data/load-data.sh"
docker exec -it hive-server bash -c "beeline -u jdbc:hive2://localhost:10000 -f ./hive/scripts/asvsp/HiveDWScript.sql"
docker exec -it spark-master bash -c "chmod +x ./asvsp/scripts/process-data.sh && ./asvsp/scripts/process-data.sh"
# beeline -u jdbc:postgresql://hive-metastore-postgresql/metastore -n hive -p hive (hive-server)
# DESCRIBE FORMATTED my_table;