from kafka import KafkaConsumer
from kafka.errors import KafkaError
import pickle
from QuartzMapper import QuartzMapper
from ImmediatelyWarningService import ImmediatelyWarningFacadeService
from PostMongo import PostMongo
class KafkaListener:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'osint-posts-raw', 
            bootstrap_servers='172.168.200.202:9092',  
            auto_offset_reset='earliest',  
            consumer_timeout_ms=1000,  
            value_deserializer=lambda x: pickle.loads(x) 
        )

    def listen(self):
        try:
            msg = self.consumer.poll(timeout_ms=3000)
            if not msg:
                print("Không có dữ liệu, kết nối thành công nhưng không có thông điệp.")
            else:
                posts_mongos = []
                for partition, records in msg.items():
                    for record in records:
                        try:
                            for post in record.value:
                                if isinstance(post, dict):  
                                    posts_mongos.append(PostMongo(**post))  
                        except Exception as e:
                            print(f"Lỗi khi xử lý record: {e}")
                            continue

                self.handle_immediately_warning(posts_mongos)
        
        except Exception as e:
            print(f"Đã có lỗi khi xử lý dữ liệu từ Kafka: {e}")
        finally:
            self.consumer.close()

    def handle_immediately_warning(self, posts_mongos):
        print(f"Processing {len(posts_mongos)} posts")
        immediately_service = ImmediatelyWarningFacadeService()
        immediately_service.handle_immediately_warning(posts_mongos)


if __name__ == '__main__':
    listener = KafkaListener()
    listener.listen()
