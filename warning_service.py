from pymongo import MongoClient
from typing import List, Dict
from enum import Enum
from dataclasses import dataclass
from typing import Optional
import uuid

@dataclass
class BaseEntity:
    id: str
    created_at: Optional[int] 
    updated_at: Optional[int] 
    created_by: Optional[str]
    updated_by: Optional[str]

    def __repr__(self):
        return f"BaseEntity(id={self.id}, created_at={self.created_at}, updated_at={self.updated_at}, created_by={self.created_by}, updated_by={self.updated_by})"
@dataclass
class WarningMsgResponse:
    id: str
    post_id: str
    warning_id: str
    warning_his_id: str
    channel: str
    content_post: str
    interactive_amount: Optional[int] = None
    discussion_amount: Optional[int] = None
    author_discussion_amount: Optional[int] = None
    like_amount: Optional[int] = None
    comment_amount: Optional[int] = None
    share_amount: Optional[int] = None
    post_link: Optional[str] = None
    author_link: Optional[str] = None
    post_amount: Optional[int] = None
    negative_amount: Optional[int] = None
    author: Optional[str] = None
    avatar_key: Optional[str] = None
    sum_reaction: Optional[int] = None
    avatar_link: Optional[str] = None
    topic_keywords: Optional[List[str]] = None

    @staticmethod
    def of(id: str, post_id: str, warning_id: str, warning_his_id: str, channel: str, content_post: str,
           interactive_amount: Optional[int], discussion_amount: Optional[int], author_discussion_amount: Optional[int],
           like_amount: Optional[int], comment_amount: Optional[int], share_amount: Optional[int],
           post_link: Optional[str], author_link: Optional[str], post_amount: Optional[int] = None,
           negative_amount: Optional[int] = None, author: Optional[str] = None, avatar_key: Optional[str] = None,
           sum_reaction: Optional[int] = None, avatar_link: Optional[str] = None, topic_keywords: Optional[List[str]] = None):
        return WarningMsgResponse(
            id=id,
            post_id=post_id,
            warning_id=warning_id,
            warning_his_id=warning_his_id,
            channel=channel,
            content_post=content_post,
            interactive_amount=interactive_amount,
            discussion_amount=discussion_amount,
            author_discussion_amount=author_discussion_amount,
            like_amount=like_amount,
            comment_amount=comment_amount,
            share_amount=share_amount,
            post_link=post_link,
            author_link=author_link or "unknown_author_link",  # 🔹 Đặt giá trị mặc định nếu None
            post_amount=post_amount,
            negative_amount=negative_amount,
            author=author,
            avatar_key=avatar_key,
            sum_reaction=sum_reaction,
            avatar_link=avatar_link,
            topic_keywords=topic_keywords
        )
@dataclass
class WarningMsgRequest:
    warning_id: str
    warning_his_id: str
    channel: str
    content_post: str
    post_id: Optional[str] = None
    interactive_amount: Optional[int] = None
    discussion_amount: Optional[int] = None
    author_discussion_amount: Optional[int] = None
    like_amount: Optional[int] = None
    comment_amount: Optional[int] = None
    share_amount: Optional[int] = None
    post_link: Optional[str] = None
    author_link: Optional[str] = None
    post_amount: Optional[int] = None
    negative_amount: Optional[int] = None
    author: Optional[str] = None
    avatar_key: Optional[str] = None

    @staticmethod
    def of(
        warning_id: str, warning_his_id: str, channel: str, content_post: str,
        interactive_amount: Optional[int] = None, discussion_amount: Optional[int] = None,
        author_discussion_amount: Optional[int] = None, like_amount: Optional[int] = None,
        comment_amount: Optional[int] = None, share_amount: Optional[int] = None,
        post_link: Optional[str] = None, author_link: Optional[str] = None
    ) -> 'WarningMsgRequest':
        return WarningMsgRequest(
            warning_id=warning_id,
            warning_his_id=warning_his_id,
            channel=channel,
            content_post=content_post,
            interactive_amount=interactive_amount,
            discussion_amount=discussion_amount,
            author_discussion_amount=author_discussion_amount,
            like_amount=like_amount,
            comment_amount=comment_amount,
            share_amount=share_amount,
            post_link=post_link,
            author_link=author_link
        )

    @staticmethod
    def of_with_post_id(
        post_id: str, warning_id: str, warning_his_id: str, channel: str, content_post: str,
        interactive_amount: Optional[int] = None, discussion_amount: Optional[int] = None,
        author_discussion_amount: Optional[int] = None, like_amount: Optional[int] = None,
        comment_amount: Optional[int] = None, share_amount: Optional[int] = None,
        post_link: Optional[str] = None, author_link: Optional[str] = None
    ) -> 'WarningMsgRequest':
        return WarningMsgRequest(
            post_id=post_id,
            warning_id=warning_id,
            warning_his_id=warning_his_id,
            channel=channel,
            content_post=content_post,
            interactive_amount=interactive_amount,
            discussion_amount=discussion_amount,
            author_discussion_amount=author_discussion_amount,
            like_amount=like_amount,
            comment_amount=comment_amount,
            share_amount=share_amount,
            post_link=post_link,
            author_link=author_link
        )

@dataclass
class WarningHistoryResponse:
    id: str
    title: str
    content: str
    amount_post: int
    type: int
    created_at: int  
    criteria: int
    status: int
    created_by: str
    is_read: bool
    tracing_id: str
    link: Optional[str] = None 

    @staticmethod
    def of(id: str, title: str, content: str, amount_post: int, type: int, created_at: int, criteria: int,
           status: int, created_by: str, is_read: bool, tracing_id: str, link: Optional[str] = None):
        return WarningHistoryResponse(
            id=id,
            title=title,
            content=content,
            amount_post=amount_post,
            type=type,
            created_at=created_at,
            criteria=criteria,
            status=status,
            created_by=created_by,
            is_read=is_read,
            tracing_id=tracing_id,
            link=link
        )

    def __repr__(self):
        return (f"WarningHistoryResponse(id={self.id}, title={self.title}, content={self.content}, "
                f"amount_post={self.amount_post}, type={self.type}, created_at={self.created_at}, "
                f"criteria={self.criteria}, status={self.status}, created_by={self.created_by}, "
                f"is_read={self.is_read}, tracing_id={self.tracing_id})")

    def to_dict(self):
        """Chuyển đổi đối tượng thành từ điển (dictionary)"""
        return {
            "id": self.id,
            "title": self.title,
            "content": self.content,
            "amount_post": self.amount_post,
            "type": self.type,
            "link": self.link,
            "created_at": self.created_at,
            "criteria": self.criteria,
            "status": self.status,
            "created_by": self.created_by,
            "is_read": self.is_read,
            "tracing_id": self.tracing_id
        }

@dataclass
class WarningHistoryRequest:
    criteria: int
    type: int
    title: str
    content: str
    amount_post: int = None
    link: str = None
    status: int = None
    created_by: str = None ,
    created_at: int = None,
    updated_by: str = None,
    updated_at: int = None,
    tracing_id: str = None
    is_notification: bool = False

    @staticmethod
    def of(criteria, type, title, content, created_by,created_at ,updated_by, updated_at, is_notification):
        return WarningHistoryRequest(
            criteria=criteria,
            type=type,
            title=title,
            content=content,
            created_by=created_by,
            created_at=created_at,
            updated_at= updated_at,
            updated_by=updated_by,
            is_notification=is_notification
        )

class WarningCriteria(Enum):
    TOPIC = 0
    KEYWORD = 1
    OBJECT = 2
    NONE = 3

    @staticmethod
    def value_of(value: int):
        for criteria in WarningCriteria:
            if criteria.value == value:
                return criteria
        raise ValueError(f"Invalid WarningCriteria value: {value}")

class WarningType(Enum):
    UNEXPECTED = 0
    PERIODIC = 1
    TRACING = 2

    @staticmethod
    def value_of(value: int):
        try:
            return WarningType(value)
        except ValueError:
            raise ValueError(f"Invalid WarningType value: {value}")
class FrequencyType(Enum):
    DAY = 1
    WEEK = 2
    MONTH = 3


    @staticmethod
    def value_of(value: int):
        try:
            return FrequencyType(value)
        except ValueError:
            raise ValueError(f"Invalid FrequencyType value: {value}")
class WarningStatus(Enum):
    ACTIVE = 0
    INACTIVE = 1

    @staticmethod
    def value_of(value: int):
        try:
            return WarningStatus(value)
        except ValueError:
            raise ValueError(f"Invalid WarningStatus value: {value}")
class WarningMethod(Enum):
    NOTIFICATION = 0
    SMS = 1
    EMAIL = 2

    @staticmethod
    def value_of(value: int):
        try:
            return WarningMethod(value)
        except ValueError:
            raise ValueError(f"Invalid WarningMethod value: {value}")
# @dataclass
# class WarningHistory(BaseEntity):
#     criteria: WarningCriteria 
#     type: WarningType       
#     title: str
#     content: str
#     amount_post: Optional[int]
#     status: Optional[int]
#     is_notification: Optional[bool]
#     is_read: Optional[bool]
#     tracing_id: Optional[str]

#     def __repr__(self):
#         return (f"WarningHistory(id={self.id}, criteria={self.criteria}, "
#                 f"type={self.type}, title={self.title}, content={self.content}, "
#                 f"amount_post={self.amount_post}, status={self.status}, "
#                 f"is_notification={self.is_notification}, is_read={self.is_read}, "
#                 f"tracing_id={self.tracing_id})")
@dataclass
class WarningHistory(BaseEntity):
    criteria: int 
    type: int       
    title: str
    content: str
    amount_post: Optional[int]
    status: Optional[int]
    is_notification: Optional[bool]
    is_read: Optional[bool]
    tracing_id: Optional[str]

    def __repr__(self):
        return (f"WarningHistory(id={self.id}, criteria={self.criteria}, "
                f"type={self.type}, title={self.title}, content={self.content}, "
                f"amount_post={self.amount_post}, status={self.status}, "
                f"is_notification={self.is_notification}, is_read={self.is_read}, "
                f"tracing_id={self.tracing_id})")

    def to_dict(self):
        """Chuyển đối tượng thành dictionary"""
        return {
            "_id": self.id,
            "criteria": self.criteria,
            "type": self.type,
            "title": self.title,
            "content": self.content,
            "created_at": self.created_at,
            "created_by": self.created_by,
            "updated_at": self.updated_at,
            "updated_by": self.updated_by
        }

class WarningHistoryService:
    def __init__(self, db_url, db_name):
        self.client = MongoClient(db_url)
        self.db = self.client[db_name]
        self.collection = self.db['warning_history']

    def create(self, request: WarningHistoryRequest):
        warning_history = self.to_entity(request)
        saved_warning_history = self.collection.insert_one(warning_history.to_dict())  # Đảm bảo dữ liệu hợp lệ
        return self.to_dto(warning_history)

    def to_entity(self, request: WarningHistoryRequest) -> WarningHistory:
        # criteria = WarningCriteria.value_of(request.criteria)
        # type = WarningType.value_of(request.type)
        warning_history = WarningHistory(
            id=str(uuid.uuid4()),  # Tạo một UUID ngẫu nhiên làm id
            criteria=request.criteria,
            type=request.type,
            title=request.title,
            content=request.content,
            amount_post=request.amount_post,
            status=request.status,
            is_notification=request.is_notification,
            is_read=False,  
            tracing_id=request.tracing_id,
            created_at=request.created_at,
            created_by=request.created_by, 
            updated_at= request.updated_at,
            updated_by=request.updated_by
        )
        return warning_history

    def to_dto(self, warning_history: WarningHistory) -> dict:
        return WarningHistoryResponse.of(
            warning_history.id,
            warning_history.title,
            warning_history.content,
            warning_history.amount_post,
            # warning_history.type.value,
            warning_history.type,

            warning_history.created_at,
            # warning_history.criteria.value,
            warning_history.criteria,

            warning_history.status,
            warning_history.created_by,
            warning_history.is_read,
            warning_history.tracing_id
        )

# @dataclass
# class Warning(BaseEntity):
#     id: str
#     name: str
#     code: str
#     cron_schedule: str
#     type: WarningType
#     frequency_type: FrequencyType
#     status: WarningStatus
#     methods: List[WarningMethod]
#     email: str
#     phone_number: str
#     is_deleted: bool

#     def __repr__(self):
#         return f"Warning(id={self.id}, name={self.name}, code={self.code}, cron_schedule={self.cron_schedule})"
@dataclass
class Warning(BaseEntity):
    id: str
    name: str
    code: str
    cron_schedule: str
    type: int
    status: int
    methods: List[int]
    email: str
    phone_number: str
    is_deleted: bool

    def __repr__(self):
        return f"Warning(id={self.id}, name={self.name}, code={self.code}, cron_schedule={self.cron_schedule})"

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
    def find(self, warning_id: str) -> Optional[Warning]:
        # Tìm kiếm cảnh báo bằng warning_id
        warning_data = self.collection.find_one({"_id": warning_id, "is_deleted": False})
        
        if warning_data:
            # return Warning(
            #     id=str(warning_data["_id"]),
            #     name=warning_data["name"],
            #     code=warning_data["code"],
            #     cron_schedule=warning_data["cron_schedule"],
            #     type=WarningType(warning_data["type"]),
            #     frequency_type=FrequencyType(warning_data["frequency_type"]),
            #     status=WarningStatus(warning_data["status"]),
            #     methods=[WarningMethod(method) for method in warning_data["methods"]],
            #     email=warning_data["email"],
            #     phone_number=warning_data["phone_number"],
            #     is_deleted=warning_data["is_deleted"]
            # )
            return Warning(
                id=str(warning_data["_id"]),
                name=warning_data["name"],
                code=warning_data["code"],
                cron_schedule=warning_data["cron_schedule"],
                type=warning_data["type"],
                # frequency_type=warning_data["frequency_type"]),
                status=warning_data["status"],
                methods=[method for method in warning_data["methods"]],
                email=warning_data.get("email", ""),
                phone_number=warning_data.get("phone_number", ""),
                is_deleted=warning_data["is_deleted"],
                created_at = warning_data["created_at"],
                updated_at = warning_data["updated_at"],
                created_by = warning_data["created_by"],
                updated_by = warning_data["updated_by"],


            )

        else:
            raise Exception("Warning not found")
@dataclass
class WarningMsg:
    post_id: str
    warning_id: str
    warning_his_id: str
    channel: str
    content_post: str
    interactive_amount: Optional[int] = None
    discussion_amount: Optional[int] = None
    author_discussion_amount: Optional[int] = None
    like_amount: Optional[int] = None
    comment_amount: Optional[int] = None
    share_amount: Optional[int] = None
    post_link: Optional[str] = None
    author_link: Optional[str] = None
    post_amount: Optional[int] = None
    negative_amount: Optional[int] = None
    author: Optional[str] = None
    avatar_key: Optional[str] = None

    def to_dict(self):
        """Chuyển đổi đối tượng thành dictionary, bỏ qua các giá trị None"""
        return {
            "post_id": self.post_id,
            "warning_id": self.warning_id,
            "warning_his_id": self.warning_his_id,
            "channel": self.channel,
            "content_post": self.content_post,
            "interactive_amount": self.interactive_amount,
            "discussion_amount": self.discussion_amount,
            "author_discussion_amount": self.author_discussion_amount,
            "like_amount": self.like_amount,
            "comment_amount": self.comment_amount,
            "share_amount": self.share_amount,
            "post_link": self.post_link,
            "author_link": self.author_link,
            "post_amount": self.post_amount,
            "negative_amount": self.negative_amount,
            "author": self.author,
            "avatar_key": self.avatar_key
        }

    def __repr__(self):
        return (f"WarningMsg(post_id={self.post_id}, warning_id={self.warning_id}, "
                f"warning_his_id={self.warning_his_id}, channel={self.channel}, content_post={self.content_post}, "
                f"interactive_amount={self.interactive_amount}, discussion_amount={self.discussion_amount}, "
                f"author_discussion_amount={self.author_discussion_amount}, like_amount={self.like_amount}, "
                f"comment_amount={self.comment_amount}, share_amount={self.share_amount}, post_link={self.post_link}, "
                f"author_link={self.author_link}, post_amount={self.post_amount}, negative_amount={self.negative_amount}, "
                f"author={self.author}, avatar_key={self.avatar_key})")

class WarningMsgService:
    def __init__(self, db_url: str, db_name: str):
        self.client = MongoClient(db_url)
        self.db = self.client[db_name]
        self.collection = self.db['warning_msg']  # MongoDB collection for warning messages

    def create(self, request: WarningMsgRequest) -> WarningMsgResponse:
        print(f"=== Start create WarningMsgRequest: {request}")

        warning_msg = self.to_entity(request)

        saved_warning_msg = self.collection.insert_one(warning_msg.to_dict())

        return self.to_dto(warning_msg)

    def to_entity(self, request: WarningMsgRequest) -> 'WarningMsg':
        warning_msg = WarningMsg(
            post_id=request.post_id,
            warning_id=request.warning_id,
            warning_his_id=request.warning_his_id,
            channel=request.channel,
            content_post=request.content_post,
            interactive_amount=request.interactive_amount,
            discussion_amount=request.discussion_amount,
            author_discussion_amount=request.author_discussion_amount,
            like_amount=request.like_amount,
            comment_amount=request.comment_amount,
            share_amount=request.share_amount,
            post_link=request.post_link,
            author_link=request.author_link,
            post_amount=request.post_amount,
            negative_amount=request.negative_amount,
            author=request.author,
            avatar_key=request.avatar_key
        )
        return warning_msg

    def to_dto(self, warning_msg: 'WarningMsg') -> WarningMsgResponse:
        # Convert WarningMsg entity to WarningMsgResponse DTO
        return WarningMsgResponse.of(
            id=str(uuid.uuid4()),  # Tạo một UUID ngẫu nhiên làm id
            post_id=warning_msg.post_id,
            warning_id=warning_msg.warning_id,
            warning_his_id=warning_msg.warning_his_id,
            channel=warning_msg.channel,
            content_post=warning_msg.content_post,
            interactive_amount=warning_msg.interactive_amount,
            discussion_amount=warning_msg.discussion_amount,
            author_discussion_amount=warning_msg.author_discussion_amount,
            like_amount=warning_msg.like_amount,
            comment_amount=warning_msg.comment_amount,
            share_amount=warning_msg.share_amount,
            post_link=warning_msg.post_link,
            author_link=warning_msg.author_link,  # 🔹 Đã bổ sung đúng thứ tự
            post_amount=warning_msg.post_amount,
            negative_amount=warning_msg.negative_amount,
            author=warning_msg.author,
            avatar_key=warning_msg.avatar_key,
            sum_reaction=None,  # 🔹 Nếu không có giá trị, có thể đặt là None
            avatar_link=None,  # 🔹 Nếu không có giá trị, có thể đặt là None
            topic_keywords=None  # 🔹 Nếu không có giá trị, có thể đặt là None
        )
# Lớp ThresholdObject
class ThresholdObject:
    def __init__(self, key: str, operator: str, value: str):
        self.key = key
        self.operator = operator
        self.value = value

    def __repr__(self):
        return f"ThresholdObject(key={self.key}, operator={self.operator}, value={self.value})"


class ConditionObjectResponse:
    def __init__(self, type: int, thresholdObjects: List[ThresholdObject]):
        self.type = type
        self.thresholdObjects = thresholdObjects

    def __repr__(self):
        return f"ConditionObjectResponse(type={self.type}, thresholdObjects={self.thresholdObjects})"


class TopicObject:
    def __init__(self, name: str, id: str, keyword: str):
        self.name = name
        self.id = id
        self.keyword = keyword

    def __repr__(self):
        return f"TopicObject(name={self.name}, id={self.id}, keyword={self.keyword})"
class TopicV2Service:
    def __init__(self, db_url: str, db_name: str):
        self.client = MongoClient(db_url)
        self.db = self.client[db_name]
        self.collection = self.db['topic_v2']  
    def get_topic_names(self, ids: List[str]) -> List[str]:
        pipeline = [
            {"$match": {"_id": {"$in": ids}}}, 
            {"$project": {"_id": 0, "name": 1}} 
        ]
        result = self.collection.aggregate(pipeline)
        
        return [doc['name'] for doc in result]


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

    # def find_by_warning_ids(self, warning_ids: List[str]) -> List[WarningConditionResponse]:
    #     # Truy vấn MongoDB để lấy danh sách các WarningConditionResponse theo warning_ids
    #     pipeline = [
    #         {"$match": {"warning_id": {"$in": warning_ids}, "is_deleted": False}}  # Lọc theo warning_id và is_deleted
    #     ]
        
    #     result = self.collection.aggregate(pipeline)
    #     warning_conditions = []
    #     for doc in result:
    #         # Chuyển đổi dữ liệu MongoDB thành các đối tượng WarningConditionResponse
    #         if "_id" not in doc:
    #             print(f"Warning: Document {doc} không chứa _id")
    #             continue  # Hoặc bạn có thể xử lý khác ở đây nếu cần

    #         condition_objects = [
    #             ConditionObjectResponse(
    #                 type=condition['type'],
    #                 thresholdObjects=[ThresholdObject(threshold['key'], threshold['operator'], threshold['value']) for threshold in condition['thresholds']]
    #             )
    #             for condition in doc['conditions']
    #         ]

    #         # topics = [TopicObject(topic['name'], topic['id'], topic['keyword']) for topic in doc.get('topics', [])]
    #         topics = [
    #             TopicObject(
    #                 topic.get('name', 'default_name'),  # Kiểm tra nếu thiếu tên, sẽ gán mặc định
    #                 topic.get('id', 'unknown_id'),  # Nếu thiếu id, sẽ gán mặc định "unknown_id"
    #                 topic.get('keyword', 'default_keyword')  # Nếu thiếu keyword, sẽ gán mặc định
    #             )
    #             for topic in doc.get('topics', [])  # Nếu không có 'topics', sử dụng danh sách rỗng
    #         ]

    #         warning_conditions.append(WarningConditionResponse(
    #             id=str(doc['_id']),
    #             warning_id=doc['warning_id'],
    #             # code=doc['code'],
    #             code=doc.get('code', 'default_code'),  # Sử dụng .get() thay vì truy cập trực tiếp

    #             criteria=doc['criteria'],
    #             conditionObjectResponses=condition_objects,
    #             topics=topics,
    #             keywords=doc['keywords'],
    #             threshold_time=doc['threshold_time'],
    #             # threshold_time_amount=doc['threshold_time_amount'],
    #             threshold_time_amount=doc.get('threshold_time_amount', 'default_threshold_time_amount'),  # Sử dụng .get() thay vì truy cập trực tiếp

    #             is_deleted=doc['is_deleted']
    #         ))

    #     return warning_conditions
    def find_by_warning_ids(self, warning_ids: List[str]) -> List[WarningConditionResponse]:
        pipeline = [
            {"$match": {"warning_id": {"$in": warning_ids}, "is_deleted": False}}
        ]
        
        result = self.collection.aggregate(pipeline)
        warning_conditions = []
        
        for doc in result:
            # Kiểm tra sự tồn tại của tất cả các trường quan trọng
            required_fields = ["_id", "warning_id", "conditions", "keywords", "threshold_time", "threshold_time_amount"]
            if not all(field in doc for field in required_fields):
                print(f"Skipping document {doc.get('_id', 'unknown_id')} due to missing fields.")
                continue  # Bỏ qua document nếu thiếu trường quan trọng

            # Kiểm tra xem 'conditions' có hợp lệ không
            if not isinstance(doc["conditions"], list) or not doc["conditions"]:
                print(f"Skipping document {doc['_id']} due to empty or invalid 'conditions'.")
                continue  # Bỏ qua document nếu 'conditions' không phải danh sách hợp lệ

            condition_objects = []
            for condition in doc["conditions"]:
                # Kiểm tra điều kiện có các trường cần thiết không
                if "type" not in condition or "thresholds" not in condition:
                    print(f"Skipping condition in document {doc['_id']} due to missing 'type' or 'thresholds'.")
                    continue  # Bỏ qua nếu thiếu 'type' hoặc 'thresholds'

                if not isinstance(condition["thresholds"], list) or not condition["thresholds"]:
                    print(f"Skipping condition in document {doc['_id']} due to empty or invalid 'thresholds'.")
                    continue  # Bỏ qua nếu 'thresholds' không hợp lệ

                threshold_objects = []
                for threshold in condition["thresholds"]:
                    # Kiểm tra từng threshold có đủ key cần thiết không
                    if "key" not in threshold or "operator" not in threshold or "value" not in threshold:
                        print(f"Skipping threshold in document {doc['_id']} due to missing fields.")
                        continue  # Bỏ qua threshold không hợp lệ
                    
                    # Thêm threshold hợp lệ vào danh sách
                    threshold_objects.append(
                        ThresholdObject(threshold["key"], threshold["operator"], threshold["value"])
                    )

                # Nếu threshold_objects trống, bỏ qua condition này
                if not threshold_objects:
                    continue

                # Thêm condition hợp lệ vào danh sách
                condition_objects.append(
                    ConditionObjectResponse(type=condition["type"], thresholdObjects=threshold_objects)
                )

            # Nếu condition_objects trống, bỏ qua document này
            if not condition_objects:
                print(f"Skipping document {doc['_id']} due to all conditions being invalid.")
                continue

            # Kiểm tra topics (nếu có)
            topics = []
            if "topics" in doc and isinstance(doc["topics"], list):
                for topic in doc["topics"]:
                    if "name" in topic and "id" in topic and "keyword" in topic:
                        topics.append(TopicObject(topic["name"], topic["id"], topic["keyword"]))

            # Thêm document hợp lệ vào danh sách trả về
            warning_conditions.append(WarningConditionResponse(
                id=str(doc["_id"]),
                warning_id=doc["warning_id"],
                code=doc["code"] if "code" in doc else None,  # Nếu thiếu 'code', gán None
                criteria=doc["criteria"] if "criteria" in doc else None,  # Nếu thiếu 'criteria', gán None
                conditionObjectResponses=condition_objects,
                topics=topics,
                keywords=doc["keywords"],
                threshold_time=doc["threshold_time"],
                threshold_time_amount=doc["threshold_time_amount"],
                is_deleted=doc["is_deleted"]
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
@dataclass
class WarningSensitiveAccountResponse:
    """
    Class biểu diễn thông tin tài khoản nhạy cảm trong cảnh báo.
    """
    author: Optional[str] = None
    author_link: Optional[str] = None
    chanel: Optional[str] = None
    post_amount: Optional[int] = None
    interaction_amount: Optional[int] = None
    discussion_amount: Optional[int] = None
    negative_amount: Optional[int] = None
    avatar_key: Optional[str] = None

# Ví dụ về sử dụng
if __name__ == "__main__":
    db_uri = "mongodb://localhost:27017"  # Địa chỉ MongoDB của bạn
    db_name = "osint"  # Tên database của bạn

    warning_service = WarningService(db_uri, db_name)

    # Lấy các warning ids có trạng thái = 0, loại = 0, và chưa bị xóa
    active_immediately_warning_ids = warning_service.list_active_immediately_warning_ids()
    print("Active Immediately Warning IDs:", active_immediately_warning_ids)
