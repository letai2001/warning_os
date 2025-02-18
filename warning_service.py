from pymongo import MongoClient
from typing import List, Dict

class WarningService:
    def __init__(self, db_url, db_name):
        self.client = MongoClient(db_url)
        self.db = self.client[db_name]
        self.collection = self.db['warnings']  

    def list_active_immediately_warning_ids(self):
        pipeline = [
            {"$match": {"status": 0, "type": 0, "is_deleted": False}},  # Trạng thái = 0, Loại = 0, và chưa xóa
            {"$project": {"_id": 1}}  # Chỉ lấy trường _id
        ]
        result = self.collection.aggregate(pipeline)
        return [str(doc['_id']) for doc in result]
# Lớp ThresholdObject
class ThresholdObject:
    def __init__(self, key: str, operator: str, value: str):
        self.key = key
        self.operator = operator
        self.value = value

    def __repr__(self):
        return f"ThresholdObject(key={self.key}, operator={self.operator}, value={self.value})"


# Lớp ConditionObjectResponse
class ConditionObjectResponse:
    def __init__(self, type: int, thresholdObjects: List[ThresholdObject]):
        self.type = type
        self.thresholdObjects = thresholdObjects

    def __repr__(self):
        return f"ConditionObjectResponse(type={self.type}, thresholdObjects={self.thresholdObjects})"


# Lớp TopicObject
class TopicObject:
    def __init__(self, name: str, id: str, keyword: str):
        self.name = name
        self.id = id
        self.keyword = keyword

    def __repr__(self):
        return f"TopicObject(name={self.name}, id={self.id}, keyword={self.keyword})"


# Lớp WarningConditionResponse
class WarningConditionResponse:
    def __init__(self, id: str, warning_id: str, code: str, criteria: int, conditionObjectResponses: List[ConditionObjectResponse], topics: List[TopicObject], keywords: List[str], threshold_time: int, threshold_time_amount: int, is_deleted: bool):
        self.id = id
        self.warning_id = warning_id
        self.code = code
        self.criteria = criteria
        self.conditionObjectResponses = conditionObjectResponses
        self.topics = topics
        self.keywords = keywords
        self.threshold_time = threshold_time
        self.threshold_time_amount = threshold_time_amount
        self.is_deleted = is_deleted

    def __repr__(self):
        return f"WarningConditionResponse(id={self.id}, warning_id={self.warning_id}, code={self.code}, criteria={self.criteria}, threshold_time={self.threshold_time}, is_deleted={self.is_deleted})"
class WarningConditionService:
    def __init__(self, db_url: str, db_name: str):
        self.client = MongoClient(db_url)
        self.db = self.client[db_name]
        self.collection = self.db['warning_conditions']  # Tên collection trong MongoDB

    def find_by_warning_ids(self, warning_ids: List[str]) -> List[WarningConditionResponse]:
        # Truy vấn MongoDB để lấy danh sách các WarningConditionResponse theo warning_ids
        pipeline = [
            {"$match": {"warning_id": {"$in": warning_ids}, "is_deleted": False}}  # Lọc theo warning_id và is_deleted
        ]
        
        result = self.collection.aggregate(pipeline)
        warning_conditions = []
        for doc in result:
            # Chuyển đổi dữ liệu MongoDB thành các đối tượng WarningConditionResponse
            condition_objects = [
                ConditionObjectResponse(
                    type=condition['type'],
                    thresholdObjects=[ThresholdObject(threshold['key'], threshold['operator'], threshold['value']) for threshold in condition['thresholds']]
                )
                for condition in doc['conditions']
            ]

            topics = [TopicObject(topic['name'], topic['id'], topic['keyword']) for topic in doc.get('topics', [])]

            warning_conditions.append(WarningConditionResponse(
                id=str(doc['_id']),
                warning_id=doc['warning_id'],
                code=doc['code'],
                criteria=doc['criteria'],
                conditionObjectResponses=condition_objects,
                topics=topics,
                keywords=doc['keywords'],
                threshold_time=doc['threshold_time'],
                threshold_time_amount=doc['threshold_time_amount'],
                is_deleted=doc['is_deleted']
            ))

        return warning_conditions

    def fetch_warning_conditions_by_warning_ids(self, warning_ids: List[str]) -> Dict[str, List[WarningConditionResponse]]:
        # Lấy danh sách các WarningConditionResponse theo warning_ids và nhóm theo warning_id
        all_warning_conditions = self.find_by_warning_ids(warning_ids)
        
        warning_id_to_condition_map = {}
        for warning_condition in all_warning_conditions:
            if warning_condition.warning_id not in warning_id_to_condition_map:
                warning_id_to_condition_map[warning_condition.warning_id] = []
            warning_id_to_condition_map[warning_condition.warning_id].append(warning_condition)
        
        return warning_id_to_condition_map

# Ví dụ về sử dụng
if __name__ == "__main__":
    db_uri = "mongodb://localhost:27017"  # Địa chỉ MongoDB của bạn
    db_name = "osint"  # Tên database của bạn

    warning_service = WarningService(db_uri, db_name)

    # Lấy các warning ids có trạng thái = 0, loại = 0, và chưa bị xóa
    active_immediately_warning_ids = warning_service.list_active_immediately_warning_ids()
    print("Active Immediately Warning IDs:", active_immediately_warning_ids)
