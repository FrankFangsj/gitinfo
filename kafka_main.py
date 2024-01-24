# This is a sample Python script.
## the project is to know more about the kafka mechanism.
import mysql.connector
from kafka import KafkaProducer

# MySQL连接配置
mysql_config = {
    'user': 'root',
    'password': 'password',
    'host': 'localhost',
    'database': 'example',
}

# Kafka连接配置
kafka_config = {
    'bootstrap_servers': 'localhost:9092',
}

def push_data():
    # 连接MySQL数据库
    mysql_connection = mysql.connector.connect(**mysql_config)
    mysql_cursor = mysql_connection.cursor()

    # 查询数据
    mysql_cursor.execute('SELECT * FROM data')
    result = mysql_cursor.fetchall()

    # 关闭MySQL连接
    mysql_cursor.close()
    mysql_connection.close()

    # 连接Kafka
    kafka_producer = KafkaProducer(**kafka_config)

    # 推送数据到Kafka
    for row in result:
        kafka_producer.send('my-topic', str(row[0]).encode(), str(row[1]).encode())

    # 关闭Kafka连接
    kafka_producer.close()

if __name__ == '__main__':
    push_data()