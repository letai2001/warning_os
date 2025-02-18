from kafka import KafkaConsumer
import pickle

def test_kafka_connection():
    try:
        # Tạo Consumer với cấu hình kết nối tới Kafka broker
        consumer = KafkaConsumer(
            'osint-posts-raw',  # Topic mà bạn muốn kiểm tra
            bootstrap_servers='172.168.200.202:9092',  # Địa chỉ Kafka của bạn
            auto_offset_reset='earliest',  # Lấy dữ liệu bắt đầu từ đầu
            consumer_timeout_ms=1000,  # Chờ tối đa 1 giây để nhận dữ liệu
            value_deserializer=lambda x: pickle.loads(x)  # Giải mã dữ liệu bằng pickle
        )

        # Polling dữ liệu
        msg = consumer.poll(timeout_ms=1000)

        if not msg:
            print("Không có dữ liệu, kết nối thành công nhưng không có thông điệp.")
        else:
            # Lấy thông điệp từ partition và in nội dung
            for partition in msg.values():
                for record in partition:
                    # Sau khi dùng pickle, dữ liệu đã được giải mã và có thể xử lý như đối tượng Python
                    print(f"Thông điệp nhận được: {record.value}")

    except Exception as e:
        print(f"Không thể kết nối với Kafka. Lỗi: {e}")
    finally:
        # Đóng kết nối consumer sau khi kiểm tra xong
        consumer.close()

if __name__ == "__main__":
    test_kafka_connection()
