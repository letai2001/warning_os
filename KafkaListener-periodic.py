from kafka import KafkaConsumer
from kafka.errors import KafkaError
import pickle
from QuartzMapper import QuartzMapper
from schedule_service import SchedulerService
from quart_warning_job import QuartWarningJob  
import json

class KafkaListener:
    def __init__(self):
        self.consumer = KafkaConsumer(
            'osint-quartz-job-listen',  # Đổi thành topic đúng
            bootstrap_servers='172.168.200.202:9092',
            auto_offset_reset='earliest',
            consumer_timeout_ms=1000,
            value_deserializer=lambda x: pickle.loads(x)  # Giải mã dữ liệu từ Kafka
        )
        self.scheduler_service = SchedulerService()  # Dịch vụ quản lý job

    def listen(self):
        try:
            msg = self.consumer.poll(timeout_ms=3000)
            if not msg:
                print("Không có dữ liệu mới từ Kafka.")
            else:
                for partition, records in msg.items():
                    for record in records:
                        try:
                            warnings = QuartzMapper.decode_to_warning(record.value)  # Giải mã dữ liệu
                            for warning in warnings:
                                job_data = { "WARING_JOB_DATA_KEY": json.dumps(warning.__dict__) }

                                # Xóa job cũ nếu có
                                self.scheduler_service.delete_schedule_job(warning.id, "WARNING_JOB_GROUP")

                                # Tạo job mới với cron schedule
                                self.scheduler_service.create_schedule_job(
                                    QuartWarningJob,
                                    warning.id,
                                    "WARNING_JOB_GROUP",
                                    warning.cron_schedule,
                                    job_data
                                )
                        except Exception as e:
                            print(f"Lỗi khi xử lý record: {e}")
                            continue

        except KafkaError as e:
            print(f"Lỗi Kafka: {e}")
        except Exception as e:
            print(f"Lỗi xử lý dữ liệu từ Kafka: {e}")
        finally:
            self.consumer.close()

if __name__ == '__main__':
    listener = KafkaListener()
    listener.listen()
