-------------------Kafka
-cach 1
cd /opt/kafka
sudo systemctl start zookeeper
sudo systemctl start kafka
sudo systemctl status zookeeper
sudo systemctl status kafka

-cach 2
cd /opt/kafka
- chay kafka
bash kafkastart.sh

- Tao topic
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic DevOps
- Check topic
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

-------------------HADOOP
start-all.sh

UI:
Hadoop NameNode UI: http://localhost:9870	
YARN Resource Manager: http://localhost:8088

----Tao folder
hadoop fs -mkdir /user
hadoop fs -mkdir /user/checkpoint_dir
hadoop fs -mkdir /user/output

-----Download file tu hdfs ve local
hadoop fs -copyToLocal /user/output/part-00000-1491cb29-83ca-4a4a-97f9-f3496e61d998-c000.csv /home/phuoc/Documents/project/downloadHDFS


-----end
stop-all.sh


----------------------SPARK
start-master.sh
pyspark
UI: http://192.168.1.78:4040

run worker: start-worker.sh spark://phuoc:7077 --port 8001

---end
stop-worker.sh
stop-master.sh

Truy cập http://localhost:8080