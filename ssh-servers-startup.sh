docker exec namenode service ssh start
docker exec hive-server service ssh start
docker exec -it spark-master bash -c "\"/usr/sbin/sshd\""
