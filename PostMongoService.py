import logging
from typing import List, Dict, Optional
from datetime import datetime, timedelta
from pymongo import MongoClient
from PostMongo import PostMongo
from warning_service import ThresholdObject ,WarningSensitiveAccountResponse
from PostTypeService import PostTypeService
from ThresholdHelper import DateUtils, ThresholdUtils  # Cần triển khai DateUtils và ThresholdUtils
import time
DB_URL = "mongodb://localhost:27017"
DB_NAME = "osint"
CREATED_TIME_ATTRIBUTE = "created_time"
IS_COMMENT = True
TYPE_ATTRIBUTE = "type"
KEYWORD_COUNT = "keywordCounts"
KEYWORD_MAP_EXPRESSION = {
    "$map": {
        "input": None,  # Danh sách keywords sẽ thay vào đây
        "as": "keyword",
        "in": {
            "keyword": "$$keyword",
            "count": {
                "$size": {
                    "$regexFindAll": {
                        "input": {
                            "$toLower": {"$ifNull": ["$content", ""]}
                        },
                        "regex": {
                            "$concat": ["(?<!\\w)", {"$toLower": "$$keyword"}, "(?!\\w)"]
                        }
                    }
                }
            }
        }
    }
}
COUNT_CONDITION_EXPRESSION_PREFIX = {
    "$expr": {
        "$anyElementTrue": {
            "$map": {
                "input": "$keywordCounts",
                "as": "kw",
                "in": None  # Điều kiện so sánh sẽ thay thế sau
            }
        }
    }
}
CREATED_TIME = "created_time"
TYPE = "type"
AUTHOR = "author"
AVATAR_KEY = "avatar_key"
AUTHOR_LINK = "author_link"
POST_AMOUNT = "post_amount"
INTERACTION_AMOUNT = "interaction_amount"
DISCUSSION_AMOUNT = "discussion_amount"
NEGATIVE_COUNT = "negative_amount"
EVALUATE = "evaluate"
NEGATIVE = "negative"
ID_AUTHOR = "_id.author"
ID_TYPE = "_id.type"
POST_COLLECTION_NAME = "osint_posts"  # Định nghĩa chính xác tên collection trong MongoDB

# Định nghĩa biểu thức MongoDB cho tổng số tương tác và thảo luận
INTERACTION_AMOUNT_MONGO_EXPRESSION = {"$add": ["$like", "$comment", "$haha", "$love", "$wow", "$sad", "$angry", "$share"]}
DISCUSSION_AMOUNT_MONGO_EXPRESSION = {"$add": ["$comment", "$share"]}

class MongoQueryHelper:
    @staticmethod
    def build_comparation_operator_criteria(field: str, value: int, operator: str) -> Dict:
        """
        Xây dựng điều kiện so sánh cho MongoDB.
        :param field: Tên trường trong MongoDB.
        :param value: Giá trị cần so sánh.
        :param operator: Toán tử so sánh (">=", "=", "<", "<=", ">").
        :return: Một từ điển MongoDB query.
        """
        operator_map = {
            ">=": {"$gte": value},
            "=": {"$eq": value},
            "<": {"$lt": value},
            "<=": {"$lte": value},
            ">": {"$gt": value},  # Mặc định nếu không khớp với toán tử nào
        }
        return {field: operator_map.get(operator, {"$gt": value})}

class PostMongoService:
    def __init__(self):
        """ Khởi tạo kết nối MongoDB """
        self.client = MongoClient(DB_URL)
        self.db = self.client[DB_NAME]
        self.collection = self.db["post_mongo"]
        self.post_type_service = PostTypeService()
    def is_crossed_threshold(self, operator: str, threshold_value: Optional[int], actual_value: Optional[int]) -> bool:
        """
        Kiểm tra xem giá trị thực tế có vượt qua ngưỡng không.
        :param operator: Toán tử so sánh (">", ">=", "=", "<", "<=").
        :param threshold_value: Giá trị ngưỡng.
        :param actual_value: Giá trị thực tế.
        :return: True nếu giá trị thực tế thỏa mãn điều kiện, False nếu không.
        """
        if threshold_value is None or actual_value is None:
            return False  # Nếu giá trị ngưỡng hoặc giá trị thực tế bị None, trả về False

        # self.logger.info("(is_crossed_threshold) start")
        # self.logger.debug(f"(is_crossed_threshold) operator: {operator}, threshold_value: {threshold_value}, actual_value: {actual_value}")

        operator_map = {
            ">": actual_value > threshold_value,
            ">=": actual_value >= threshold_value,
            "=": actual_value == threshold_value,
            "<": actual_value < threshold_value,
            "<=": actual_value <= threshold_value
        }

        return operator_map.get(operator, False)

    def list_crossed_threshold_post_base_on_topic_id_v2(self, topic_ids: List[str], condition_type: int, 
                                                        threshold_time: int, threshold_time_amount: int, 
                                                        threshold_object: ThresholdObject, page: int, size: int) -> List[PostMongo]:
        """
        Lấy danh sách bài viết vượt ngưỡng dựa trên chủ đề.
        """
        start_time = int(time.time() * 1000) - ThresholdUtils.calculate_time_in_milliseconds(threshold_time, threshold_time_amount)
        channels = self.post_type_service.get_post_types([threshold_object.key], is_comment=False)

        if condition_type == 0:  
            return self.list_post_has_crossed_threshold_interactive_base_on_topic(topic_ids, start_time, channels, threshold_object, page, size)
        elif condition_type == 1: 
            return self.list_post_has_crossed_threshold_discussion_base_on_topic(topic_ids, start_time, channels, threshold_object, page, size)
        elif condition_type == 2:  
            return self.list_post_has_negative_evaluate_crossed_threshold(topic_ids, start_time, channels, threshold_object, page, size)
        else:
            raise ValueError(f"Unexpected value: {condition_type}")

    def list_post_has_crossed_threshold_interactive_base_on_topic(self, topic_ids: List[str], start_time: int, 
                                                                  channels: List[str], threshold_object: ThresholdObject, 
                                                                  page: int, size: int) -> List[dict]:
        """
        Lấy danh sách bài viết có mức độ tương tác vượt ngưỡng theo chủ đề.
        :param topic_ids: Danh sách ID chủ đề.
        :param start_time: Thời gian bắt đầu để lọc bài viết.
        :param channels: Danh sách kênh truyền thông.
        :param threshold_object: Đối tượng chứa điều kiện lọc.
        :param page: Số trang.
        :param size: Số lượng bài viết trên mỗi trang.
        :return: Danh sách bài viết dưới dạng dictionary.
        """
        query = {
            "created_time": {"$gte": start_time},
            "type": {"$in": channels},
            "topic_id": {"$in": topic_ids}
        }

        # Thêm điều kiện so sánh interactive_amount
        query.update(MongoQueryHelper.build_comparation_operator_criteria(
            "interactive_amount", int(threshold_object.value), threshold_object.operator
        ))

        # Truy vấn MongoDB với phân trang
        results = self.collection.find(query).skip(page * size).limit(size)

        return list(results)

    def list_post_has_crossed_threshold_discussion_base_on_topic(self, topic_ids: List[str], start_time: int, 
                                                                 channels: List[str], threshold_object: ThresholdObject, 
                                                                 page: int, size: int) -> List[dict]:
        """
        Lấy danh sách bài viết có mức độ thảo luận vượt ngưỡng theo chủ đề.
        :param topic_ids: Danh sách ID chủ đề.
        :param start_time: Thời gian bắt đầu để lọc bài viết.
        :param channels: Danh sách kênh truyền thông.
        :param threshold_object: Đối tượng chứa điều kiện lọc.
        :param page: Số trang.
        :param size: Số lượng bài viết trên mỗi trang.
        :return: Danh sách bài viết dưới dạng dictionary.
        """
        pipeline = [
            {"$match": {
                "created_time": {"$gte": start_time},
                "type": {"$in": channels},
                "topic_id": {"$in": topic_ids}
            }},
            {"$set": {
                "discussion_amount": {"$sum": ["$comment", "$share", 1]}  # Áp dụng tính toán discussion_amount
            }},
            {"$match": MongoQueryHelper.build_comparation_operator_criteria(
                "discussion_amount", int(threshold_object.value), threshold_object.operator
            )},
            {"$skip": page * size},
            {"$limit": size}
        ]

        # Thực hiện aggregation trong MongoDB
        results = self.collection.aggregate(pipeline)

        return list(results)
    def count_post_has_negative_evaluate(self, topic_ids: List[str], start_time: int, post_types: List[str]) -> int:
        """
        Đếm số lượng bài viết có đánh giá tiêu cực.
        """
        pipeline = [
            {"$match": {
                "created_time": {"$gte": start_time},
                "type": {"$in": post_types},
                "topic_id": {"$in": topic_ids},
                "evaluate": "negative"
            }},
            {"$group": {"_id": None, "count": {"$sum": 1}}},
            {"$project": {"count": 1, "_id": 0}}
        ]

        result = list(self.collection.aggregate(pipeline))
        return result[0]["count"] if result else 0

    def list_post_has_negative_evaluate_crossed_threshold(self, topic_ids: List[str], start_time: int, 
                                                          channels: List[str], threshold_object: ThresholdObject, 
                                                          page: int, size: int) -> List[dict]:
        """
        Lấy danh sách bài viết có đánh giá tiêu cực vượt ngưỡng theo chủ đề.
        """
        post_types = self.post_type_service.get_post_types([threshold_object.key], is_comment=False)
        count_negative_post = self.count_post_has_negative_evaluate(topic_ids, start_time, post_types)

        if not self.is_crossed_threshold(threshold_object.operator, int(threshold_object.value), count_negative_post):
            return []

        return self.list_post_has_negative_evaluate(topic_ids, start_time, channels, page, size)

    def list_post_has_negative_evaluate(self, topic_ids: List[str], start_time: int, channels: List[str], 
                                        page: int, size: int) -> List[dict]:
        """
        Truy vấn danh sách bài viết có đánh giá tiêu cực.
        """
        pipeline = [
            {"$match": {
                "created_time": {"$gte": start_time},
                "type": {"$in": channels},
                "topic_id": {"$in": topic_ids},
                "evaluate": "negative"
            }},
            {"$skip": page * size},
            {"$limit": size}
        ]

        results = self.collection.aggregate(pipeline)
        return list(results)
    def list_crossed_threshold_post_base_on_keywords_v2(self, keywords: List[str], threshold_time: int, 
                                                        threshold_time_amount: int, threshold_object: ThresholdObject, 
                                                        page: int, size: int) -> List[dict]:
        """
        Lấy danh sách bài viết có từ khóa vượt ngưỡng dựa trên số lần xuất hiện từ khóa.
        """
        print("=== Start (list_crossed_threshold_post_base_on_keywords_v2)")

        # Tính thời gian bắt đầu
        start_time = int(time.time() * 1000) - ThresholdUtils.calculate_time_in_milliseconds(threshold_time, threshold_time_amount)

        # Lấy danh sách post types dựa trên thresholdObject
        post_types = self.post_type_service.get_post_types([threshold_object.key], is_comment=not IS_COMMENT)

        # Xây dựng điều kiện truy vấn
        match_stage = {
            "$match": {
                CREATED_TIME_ATTRIBUTE: {"$gte": start_time}
            }
        }
        if post_types:
            match_stage["$match"][TYPE_ATTRIBUTE] = {"$in": post_types}

        # Chuyển danh sách keywords sang MongoDB format
        keyword_map_expression = KEYWORD_MAP_EXPRESSION.copy()
        keyword_map_expression["$map"]["input"] = [{"$literal": keyword} for keyword in keywords]

        # Điều kiện lọc số lượng từ khóa
        count_condition_expression = COUNT_CONDITION_EXPRESSION_PREFIX.copy()
        count_condition_expression["$expr"]["$anyElementTrue"]["$map"]["in"] = MongoQueryHelper.build_comparation_operator_criteria(
            "kw.count", int(threshold_object.value), threshold_object.operator
        )

        # Thực hiện aggregation
        pipeline = [
            match_stage,
            {"$addFields": {KEYWORD_COUNT: keyword_map_expression}},
            {"$match": count_condition_expression},
            {"$skip": page * size},
            {"$limit": size}
        ]

        results = self.collection.aggregate(pipeline)
        return list(results)
    def build_keyword_string(self, keywords: List[str]) -> List[dict]:
        """
        Chuyển danh sách từ khóa thành danh sách JSON phù hợp với MongoDB.
        """
        return [{"$literal": keyword.replace('"', '\\"')} for keyword in keywords]
    def list_crossed_threshold_account_v2(self, threshold_time: int, threshold_time_amount: int, 
                                          threshold_object: ThresholdObject) -> List[WarningSensitiveAccountResponse]:
        """
        Truy vấn danh sách tài khoản có mức độ thảo luận tiêu cực vượt ngưỡng.
        """
        print("(listCrossedThresholdAccountV2) start")

        # Tính thời gian bắt đầu (timestamp theo milliseconds)
        start_time = int(time.time() * 1000) - ThresholdUtils.calculate_time_in_milliseconds(threshold_time, threshold_time_amount)

        # Lấy danh sách post types dựa trên thresholdObject
        channels = self.post_type_service.get_post_types([threshold_object.key], is_comment=False)

        # Aggregation pipeline
        pipeline = [
            {"$match": {
                CREATED_TIME: {"$gte": start_time},
                TYPE: {"$in": channels}
            }},
            {"$group": {
                "_id": {AUTHOR: f"${AUTHOR}", TYPE: f"${TYPE}"},
                AVATAR_KEY: {"$first": f"${AVATAR_KEY}"},
                AUTHOR_LINK: {"$first": f"${AUTHOR_LINK}"},
                POST_AMOUNT: {"$sum": 1},  # Đếm số lượng bài viết
                INTERACTION_AMOUNT: {"$sum": INTERACTION_AMOUNT_MONGO_EXPRESSION},  # Tổng số tương tác
                DISCUSSION_AMOUNT: {"$sum": DISCUSSION_AMOUNT_MONGO_EXPRESSION},  # Tổng số bình luận và chia sẻ
                NEGATIVE_COUNT: {
                    "$sum": {
                        "$cond": [{"$eq": [f"${EVALUATE}", NEGATIVE]}, 1, 0]  # Đếm số lượng bài có đánh giá tiêu cực
                    }
                }
            }},
            {"$match": MongoQueryHelper.build_comparation_operator_criteria(
                NEGATIVE_COUNT, int(threshold_object.value), threshold_object.operator
            )},
            {"$project": {
                "_id": 0,
                "author": f"${ID_AUTHOR}",
                "type": f"${ID_TYPE}",
                AUTHOR_LINK: 1,
                POST_AMOUNT: 1,
                INTERACTION_AMOUNT: 1,
                DISCUSSION_AMOUNT: 1,
                NEGATIVE_COUNT: 1,
                AVATAR_KEY: 1
            }}
        ]

        results = self.collection.aggregate(pipeline)
        return [WarningSensitiveAccountResponse(**doc) for doc in results]
