cd ./asvsp/scripts
./../../spark/bin/spark-submit ./pre-process.py
./../../spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./location-dims.py
./../../spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./time-dims.py
./../../spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./gas-dim.py
./../../spark/bin/spark-submit --packages org.postgresql:postgresql:42.2.10 ./gas-emission-fact.py