from kafka import KafkaConsumer
import pickle
import logging

# Cấu hình logging

# Cấu hình Kafka Consumer
bootstrap_servers = '172.168.200.202:9092'
topic_name = 'osint-posts-raw'

def check_kafka_messages():
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='latest',  # Lấy các tin nhắn mới nhất
        enable_auto_commit=False,     # Không tự động commit offset
        value_deserializer=lambda x: pickle.loads(x),
        consumer_timeout_ms=5000     # Dừng lại sau 5 giây nếu không có message
    )


    try:
        for message in consumer:
            print(f"Nhận được message từ Kafka: {message.value}")
    
    except Exception as e:
        print(f"Lỗi khi kết nối tới Kafka: {e}")

    finally:
        consumer.close()

if __name__ == "__main__":
    check_kafka_messages()
