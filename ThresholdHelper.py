# from abc import ABC, abstractmethod
from typing import List, Dict ,Optional
from PostMongo import PostMongo
from PostTypeService import PostTypeResponse
from warning_service import WarningConditionResponse, ThresholdObject, WarningSensitiveAccountResponse, WarningCriteria ,TopicObject, ConditionObjectResponse
from dataclasses import dataclass, field
from enum import Enum
from PostMongoService import PostMongoService
class ThresholdTime(Enum):
    HOUR = 0
    DAY = 1
    WEEK = 2
    MONTH = 3
    YEAR = 4

    @staticmethod
    def from_value(value: int):
        """ Chuyển đổi giá trị số thành ThresholdTime Enum. """
        for time in ThresholdTime:
            if time.value == value:
                return time
        raise ValueError(f"Invalid ThresholdTime value: {value}")

@dataclass
class CrossedThresholdObjectDTO:
    """
    Class chứa thông tin về các bài viết hoặc tài khoản vượt ngưỡng cảnh báo.
    """
    criteria: Optional[int] = None
    sensitive_accounts: List[WarningSensitiveAccountResponse] = field(default_factory=list)
    exceed_threshold_topic_post: List[PostMongo] = field(default_factory=list)
    exceed_threshold_keyword_post: List[PostMongo] = field(default_factory=list)

class ThresholdUtils:
    @staticmethod
    def is_crossed_threshold(operator: str, threshold_value: int, value: int) -> bool:
        if operator == ">":
            return value > threshold_value
        elif operator == ">=":
            return value >= threshold_value
        elif operator == "=":
            return value == threshold_value
        elif operator == "<":
            return value < threshold_value
        elif operator == "<=":
            return value <= threshold_value
        else:
            return False
    @staticmethod
    def calculate_time_in_milliseconds(threshold_time: ThresholdTime, amount: int) -> int:
        """
        Tính toán thời gian dưới dạng mili giây dựa trên loại thời gian (giờ, ngày, tuần, tháng, năm).
        """
        match threshold_time:
            case ThresholdTime.HOUR:
                return amount * 3600000  # 1 giờ = 3,600,000 ms
            case ThresholdTime.DAY:
                return amount * 24 * 3600000  # 1 ngày = 86,400,000 ms
            case ThresholdTime.WEEK:
                return amount * 7 * 24 * 3600000  # 1 tuần = 604,800,000 ms
            case ThresholdTime.MONTH:
                return amount * 30 * 24 * 3600000  # 1 tháng ~ 2,592,000,000 ms
            case ThresholdTime.YEAR:
                return amount * 365 * 24 * 3600000  # 1 năm ~ 31,536,000,000 ms
            case _:
                raise ValueError(f"Unsupported ThresholdTime: {threshold_time}")

# class ThresholdHelper(ABC):
#     @abstractmethod
#     def get_condition_if_post_pass_through_warning_condition(self, post_mongo, warning_condition, all_post_types):
#         pass

#     @abstractmethod
#     def get_crossed_threshold_object_by_topic(self, threshold_time, threshold_time_amount, topics, condition_object):
#         pass

#     @abstractmethod
#     def get_crossed_threshold_object_by_keyword(self, threshold_time, threshold_time_amount, keywords, condition_object):
#         pass
class ThresholdHelper:
    def __init__(self):
        self.post_mongo_service = PostMongoService()

    def get_condition_if_post_pass_through_warning_condition(self, post_mongo: PostMongo, warning_condition: WarningConditionResponse, all_post_types: List[PostTypeResponse]):
        if warning_condition.criteria == 0:
            return self.is_post_pass_through_topic_condition(post_mongo, warning_condition, all_post_types)
        elif warning_condition.criteria == 1:
            return self.is_post_pass_through_keyword_condition(post_mongo, warning_condition, all_post_types)
        return None

    def is_post_pass_through_topic_condition(self, post_mongo: PostMongo, warning_condition: WarningConditionResponse, all_post_types: List[PostTypeResponse]):
        channel = post_mongo.type
        topic_ids = [topic.id for topic in warning_condition.topics]

        # Kiểm tra sự trùng lặp giữa topic_ids và post_mongo.topic_ids
        if not self.has_common_element(topic_ids, post_mongo.topic_id):
            return None

        # Duyệt qua các điều kiện của warning condition
        for condition_object in warning_condition.conditionObjectResponses:
            for threshold_object in condition_object.thresholdObjects:
                post_types_of_key = self.get_post_type_by_key(threshold_object.key, all_post_types)

                if channel not in post_types_of_key:
                    continue

                # Kiểm tra điều kiện theo loại
                if condition_object.type == 0:
                    if self.is_post_pass_through_topic_condition_by_interactive(post_mongo, threshold_object):
                        return condition_object

                elif condition_object.type == 1:
                    if self.is_post_pass_through_topic_condition_by_discussion(post_mongo, threshold_object):
                        return condition_object

                # Nếu loại là NEGATIVITY hoặc mặc định
                elif condition_object.type == 2:
                    return None
                else:
                    return None

        return None
    def is_post_pass_through_topic_condition_by_interactive(self, post_mongo : PostMongo, threshold_object : ThresholdObject):
        if threshold_object.key is None or threshold_object.value is None or threshold_object.operator is None:
            return False

        threshold_value = int(threshold_object.value)

        return ThresholdUtils.is_crossed_threshold(
            threshold_object.operator,
            threshold_value,
            post_mongo.interactive
        )
    def is_post_pass_through_topic_condition_by_discussion(self, post_mongo : PostMongo, threshold_object : ThresholdObject):
        # Kiểm tra các thuộc tính cần thiết trong threshold_object
        if threshold_object.key is None or threshold_object.value is None or threshold_object.operator is None:
            return False

        threshold_value = int(threshold_object.value)

        # Tính toán tổng số bình luận và chia sẻ
        discussion_amount = post_mongo.comment + post_mongo.share

        # Nếu post được chia sẻ, cộng thêm 1 vào số lượng chia sẻ
        if post_mongo.is_shared == 1:  # Điều kiện này có thể được điều chỉnh tùy thuộc vào giá trị thực tế
            discussion_amount += 1

        # Kiểm tra nếu điều kiện ngưỡng đã vượt qua
        return ThresholdUtils.is_crossed_threshold(
            threshold_object.operator,
            threshold_value,
            discussion_amount
        )

    def is_post_pass_through_keyword_condition(self, post_mongo : PostMongo, warning_condition : WarningConditionResponse, all_post_types : List[PostTypeResponse]):
        channel = post_mongo.type
        keywords = warning_condition.keywords

        for condition_object in warning_condition.conditionObjectResponses:
            for threshold_object in condition_object.thresholdObjects:
                post_types_of_key = self.get_post_type_by_key(threshold_object.key, all_post_types)

                if channel not in post_types_of_key:
                    continue

                for keyword in keywords:
                    if not threshold_object.key or not threshold_object.value or not threshold_object.operator:
                        continue
                    
                    threshold_value = int(threshold_object.value)
                    count_occurrence_in_content = self.count_occurrence_of_keywords_in_post_content(post_mongo.content, keyword)
                    count_occurrence_in_title = self.count_occurrence_of_keywords_in_post_content(post_mongo.title, keyword)

                    if count_occurrence_in_content > 0 and ThresholdUtils.is_crossed_threshold(
                            threshold_object.operator, threshold_value, count_occurrence_in_content):
                        return condition_object

                    if count_occurrence_in_title > 0 and ThresholdUtils.is_crossed_threshold(
                            threshold_object.operator, threshold_value, count_occurrence_in_title):
                        return condition_object

        return None

    def has_common_element(self, list1, list2):
        if list1 is None or list2 is None or not list1 or not list2:
            return False
        
        for element in list1:
            if element in list2:
                return True
        return False
    def get_post_type_by_key(self,key: str, all_post_types: List[PostTypeResponse]) -> List[str]:
        post_type_keys = []

        for post_type in all_post_types:
            if post_type.key == key:
                post_type_keys.append(post_type.name)
        
        return post_type_keys
    def count_occurrence_of_keywords_in_post_content(self, content: str, keyword: str) -> int:
        count = 0
        index = 0

        # Chuyển cả nội dung và từ khóa về chữ thường để không phân biệt hoa thường
        lower_content = content.lower()
        lower_keyword = keyword.lower()

        # Sử dụng vòng lặp để tìm tất cả các vị trí xuất hiện của từ khóa trong nội dung
        while (index := lower_content.find(lower_keyword, index)) != -1:
            count += 1
            index += len(lower_keyword)  # Tiến tới vị trí sau khi tìm thấy từ khóa

        return count
    def get_crossed_threshold_object_by_topic(self, threshold_time: int, threshold_time_amount: int, 
                                              topics: List[str], condition_object: ConditionObjectResponse) -> Optional[CrossedThresholdObjectDTO]:
        """
        Kiểm tra xem có bài viết nào vượt ngưỡng dựa trên chủ đề không.
        """
        topic_ids = self.get_topic_ids(topics)

        for threshold_object in condition_object.thresholdObjects:
            if not (threshold_object.key and threshold_object.value and threshold_object.operator):
                continue  # Bỏ qua nếu thiếu key/value/operator

            crossed_threshold_posts = []  # Danh sách bài viết vượt ngưỡng
            page = 0
            size = 10

            while True:
                # Truy vấn bài viết từ MongoDB dựa trên điều kiện cảnh báo
                threshold_posts = self.post_mongo_service.list_crossed_threshold_post_base_on_topic_id_v2(
                    topic_ids, condition_object.type, threshold_time, threshold_time_amount, threshold_object, page, size
                )

                crossed_threshold_posts.extend(threshold_posts)

                if len(threshold_posts) < size:
                    break  # Dừng nếu không còn bài viết mới

                page += 1  # Sang trang tiếp theo

            if crossed_threshold_posts:
                print("(get_crossed_threshold_object_by_topic) ==> THRESHOLD")
                return CrossedThresholdObjectDTO(
                    criteria=WarningCriteria.TOPIC,
                    exceed_threshold_topic_post=crossed_threshold_posts
                )

        return None  # Nếu không có bài viết vượt ngưỡng
    @staticmethod
    def get_topic_ids(topics: List[TopicObject]) -> List[str]:
        """
        Lấy danh sách ID từ danh sách TopicObject.
        :param topics: Danh sách TopicObject.
        :return: Danh sách topic_id.
        """
        return [topic.id for topic in topics] if topics else []
    def get_crossed_threshold_object_by_keyword(self, threshold_time: int, threshold_time_amount: int, 
                                                keywords: List[str], condition_object: ConditionObjectResponse) -> Optional[CrossedThresholdObjectDTO]:
        """
        Kiểm tra xem bài viết có vượt qua ngưỡng theo từ khóa hay không.
        """
        for threshold_object in condition_object.thresholdObjects:
            page = 0
            size = 10

            # Kiểm tra điều kiện hợp lệ
            if not threshold_object.key or not threshold_object.value or not threshold_object.operator:
                continue

            crossed_threshold_posts = []

            # Lặp qua danh sách bài viết theo từ khóa với phân trang
            while True:
                posts = self.post_mongo_service.list_crossed_threshold_post_base_on_keywords_v2(
                    keywords, threshold_time, threshold_time_amount, threshold_object, page, size
                )

                crossed_threshold_posts.extend(posts)
                page += 1

                # Nếu số lượng bài viết nhỏ hơn kích thước trang, dừng vòng lặp
                if len(posts) < size:
                    break

            # Nếu có bài viết vượt ngưỡng, tạo đối tượng `CrossedThresholdObjectDTO`
            if crossed_threshold_posts:
                return CrossedThresholdObjectDTO(
                    criteria=WarningCriteria.KEYWORD,
                    exceed_threshold_keyword_post=crossed_threshold_posts
                )

        return None
    def get_crossed_threshold_object_by_object(self, threshold_time: int, threshold_time_amount: int, 
                                               condition_object: ConditionObjectResponse) -> Optional[CrossedThresholdObjectDTO]:
        """
        Kiểm tra xem tài khoản có vượt qua ngưỡng theo tiêu chí nhất định hay không.
        """
        for threshold_object in condition_object.thresholdObjects:
            # Kiểm tra nếu thresholdObject hợp lệ
            if not threshold_object.key or not threshold_object.value or not threshold_object.operator:
                continue

            # Lấy danh sách tài khoản có mức độ thảo luận tiêu cực vượt ngưỡng
            warning_sensitive_accounts = self.post_mongo_service.list_crossed_threshold_account_v2(
                threshold_time, threshold_time_amount, threshold_object
            )

            # Nếu có tài khoản vi phạm ngưỡng, tạo đối tượng `CrossedThresholdObjectDTO`
            if warning_sensitive_accounts:
                return CrossedThresholdObjectDTO(
                    criteria=WarningCriteria.OBJECT.value,
                    sensitive_accounts=warning_sensitive_accounts
                )

        return None

    # def get_crossed_threshold_object_by_topic(self, threshold_time, threshold_time_amount, topics, condition_object):
    #     topic_ids = [topic.id for topic in topics]

    #     for threshold_object in condition_object.threshold_objects:
    #         if not threshold_object.key or not threshold_object.value or not threshold_object.operator:
    #             continue

    #         crossed_threshold_posts = []
    #         page = 0
    #         size = 10

    #         while True:
    #             posts = self.post_mongo_service.list_crossed_threshold_posts_by_topic(
    #                 topic_ids, condition_object.type, threshold_time, threshold_time_amount, threshold_object, page, size)
    #             crossed_threshold_posts.extend(posts)

    #             if len(posts) < size:
    #                 break
    #             page += 1

    #         if crossed_threshold_posts:
    #             return {
    #                 'criteria': 'TOPIC',
    #                 'exceed_threshold_topic_post': crossed_threshold_posts
    #             }

    #     return None

    # def get_crossed_threshold_object_by_keyword(self, threshold_time, threshold_time_amount, keywords, condition_object):
    #     for threshold_object in condition_object.threshold_objects:
    #         if not threshold_object.key or not threshold_object.value or not threshold_object.operator:
    #             continue

    #         crossed_threshold_posts = []
    #         page = 0
    #         size = 10

    #         while True:
    #             posts = self.post_mongo_service.list_crossed_threshold_posts_by_keywords(
    #                 keywords, threshold_time, threshold_time_amount, threshold_object, page, size)
    #             crossed_threshold_posts.extend(posts)

    #             if len(posts) < size:
    #                 break
    #             page += 1

    #         if crossed_threshold_posts:
    #             return {
    #                 'criteria': 'KEYWORD',
    #                 'exceed_threshold_keyword_post': crossed_threshold_posts
    #             }

    #     return None
