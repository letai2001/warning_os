from kafka import KafkaConsumer
from kafka.errors import KafkaError
import pickle
from QuartzMapper import QuartzMapper

class KafkaListener:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'osint-posts-raw',  # Topic mà bạn muốn kiểm tra
            bootstrap_servers='172.168.200.202:9092',  # Địa chỉ Kafka của bạn
            auto_offset_reset='earliest',  # Lấy dữ liệu bắt đầu từ đầu
            consumer_timeout_ms=1000,  # Chờ tối đa 1 giây để nhận dữ liệu
            value_deserializer=lambda x: pickle.loads(x)  # Giải mã dữ liệu bằng pickle
        )

    def listen(self):
        try:
            # Polling dữ liệu từ Kafka
            for msg in self.consumer:
                if msg is None:
                    print("Không có dữ liệu.")
                    continue

                if msg.error():
                    if msg.error.code() == KafkaError._PARTITION_EOF:
                        print(f"End of partition reached: {msg}")
                    else:
                        print(f"Lỗi Kafka: {msg.error()}")
                else:
                    # Duyệt qua các partition và records
                    for partition, records in msg.items():
                        for record in records:
                            # Chuyển đổi byte data thành danh sách PostMongo
                            posts_mongo = QuartzMapper.decode_to_post(record.value)
                            self.handle_immediately_warning(posts_mongo)
        except Exception as e:
            print(f"Đã có lỗi khi xử lý dữ liệu từ Kafka: {e}")
        finally:
            # Đóng kết nối Kafka consumer
            self.consumer.close()

    def handle_immediately_warning(self, posts_mongo):
        # Xử lý dữ liệu cảnh báo ngay lập tức
        print(f"Processing {len(posts_mongo)} posts")
        # Call immediatelyWarningFacadeService.handleImmediatelyWarning(posts_mongo) ở đây nếu cần

if __name__ == '__main__':
    listener = KafkaListener()
    listener.listen()
