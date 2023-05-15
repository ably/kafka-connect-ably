The current testing framework is based on Sysbench.
Steps to setup the test environment.

1) Start the connector.
```
 docker-compose -f docker-compose-development.yml up
```

2) After all the processes are up, create the Debezium MySQL Source connector.

```
./setupDebezium.sh
```
If the connector is not created successfully, it could be because MySQL or Debezium containers are not up and running.

3) Download sysbench and run the tests.

```
sysbench \
/usr/share/sysbench/oltp_insert.lua \
--report-interval=2 \
--threads=10 \
--rate=0 \
--time=${time} \
--db-driver=mysql \
--mysql-host=${mysql_host} \
--mysql-port=${mysql_port} \
--mysql-user=${mysql_username} \
--mysql-db=${mysql_db} \
--mysql-password=${mysql_password} \
--tables=1 \
--table-size=100 \

```