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
from pymongo import MongoClient
from datetime import datetime

def transfer_single_document(source_uri, target_uri, source_db, target_db, collection_name, query):
    try:
        # Kết nối đến MongoDB
        source_client = MongoClient(source_uri)
        target_client = MongoClient(target_uri)

        # Truy cập vào database và collection
        source_collection = source_client[source_db][collection_name]
        target_collection = target_client[target_db][collection_name]

        # Lấy một tài liệu từ collection nguồn theo query
        document = source_collection.find_one(query)

        if document:
            # Cập nhật các trường theo yêu cầu
            document["type"] = 1
            current_timestamp = int(datetime.utcnow().timestamp() * 1000)  # Chuyển thành ms
            document["created_at"] = current_timestamp
            document["updated_at"] = current_timestamp

            # Loại bỏ _id nếu cần (vì MongoDB không cho phép chèn tài liệu với _id đã tồn tại)
            # if "_id" in document:
            #     document.pop("_id")

            # Đẩy tài liệu vào collection đích
            target_collection.insert_one(document)
            print(f"Đã chuyển tài liệu từ {source_db}.{collection_name} sang {target_db}.{collection_name} với cập nhật mới.")
        else:
            print("Không tìm thấy tài liệu khớp với query.")

    except Exception as e:
        print(f"Đã xảy ra lỗi: {e}")

# Query để lấy một tài liệu cụ thể
def transfer_warning_condition(source_uri, target_uri, source_db, target_db, collection_name, query):
    try:
        # Kết nối đến MongoDB
        source_client = MongoClient(source_uri)
        target_client = MongoClient(target_uri)

        # Truy cập vào database và collection
        source_collection = source_client[source_db][collection_name]
        target_collection = target_client[target_db][collection_name]

        # Lấy một tài liệu từ collection nguồn theo query
        document = source_collection.find_one(query)

        if document:
            # Cập nhật các trường theo yêu cầu
            document["warning_id"] = "163452af-c41b-4df0-8925-89f9abbc5ad4"
            document["keywords"] = ["nhà"]
            current_timestamp = int(datetime.utcnow().timestamp() * 1000)  # Chuyển thành ms
            document["created_at"] = current_timestamp
            document["updated_at"] = current_timestamp

            # Loại bỏ _id nếu cần (vì MongoDB không cho phép chèn tài liệu với _id đã tồn tại)
            # if "_id" in document:
            #     document.pop("_id")

            # Đẩy tài liệu vào collection đích
            target_collection.insert_one(document)
            print(f"Đã chuyển tài liệu từ {source_db}.{collection_name} sang {target_db}.{collection_name} với cập nhật mới.")
        else:
            print("Không tìm thấy tài liệu khớp với query.")

    except Exception as e:
        print(f"Đã xảy ra lỗi: {e}")


if __name__ == "__main__":
    source_uri = "mongodb://10.11.32.22:30000"
    target_uri = "mongodb://172.168.200.202:30000"
    source_db = "osint"
    target_db = "osint"
    # collection_name = "warnings"
    collection_name = "warning_conditions"

    # Query để lấy một tài liệu cụ thể
    # query = {"_id": "163452af-c41b-4df0-8925-89f9abbc5ad4"}

    # # Gọi hàm chuyển tài liệu
    # transfer_single_document(source_uri, target_uri, source_db, target_db, collection_name, query)

    query = {"_id": "8830add8-2856-445a-b8f7-f4a1188459ea"}  # Điều chỉnh nếu cần thiết

    # Gọi hàm chuyển tài liệu
    transfer_warning_condition(source_uri, target_uri, source_db, target_db, collection_name, query)
