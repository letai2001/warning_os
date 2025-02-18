from kafka import KafkaAdminClient, KafkaConsumer
from kafka.errors import KafkaError

# Kết nối tới Kafka Broker
# bootstrap_servers = '10.11.32.21:30105'
bootstrap_servers = '172.168.200.202:9092'

def list_topics():
    try:
        # Tạo KafkaAdminClient để liệt kê các topic
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        topics = admin_client.list_topics()
        print(f"Các topic có trong Kafka: {topics}")
    except Exception as e:
        print(f"Đã có lỗi khi kết nối với Kafka: {e}")

def list_consumer_groups():
    try:
        consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
        groups = consumer.list_groups()
        print(f"Các group ID có trong Kafka: {groups}")
    except KafkaError as e:
        print(f"Đã có lỗi khi kết nối với Kafka: {e}")

if __name__ == "__main__":
    list_topics()  # Liệt kê các topic
    # list_consumer_groups()  # Liệt kê các group ID
