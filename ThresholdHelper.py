# from abc import ABC, abstractmethod
from typing import List, Dict
from PostMongo import PostMongo
from PostTypeService import PostTypeResponse
from warning_service import WarningConditionResponse, ThresholdObject
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
    # def __init__(self, post_mongo_service):
    #     self.post_mongo_service = post_mongo_service

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
        if not self.has_common_element(topic_ids, post_mongo.topic_ids):
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
        # Logic for keyword condition
        pass
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
                    count_occurrence_in_title = self.count_occurrence_of_keywords_in_post_content(post_mongo.content, keyword)

                    if count_occurrence_in_content > 0 and ThresholdUtils.is_crossed_threshold(
                            threshold_object.operator, threshold_value, count_occurrence_in_content):
                        return condition_object

                    if count_occurrence_in_title > 0 and ThresholdUtils.is_crossed_threshold(
                            threshold_object.operator, threshold_value, count_occurrence_in_title):
                        return condition_object

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
    def count_occurrence_of_keywords_in_post_content(content: str, keyword: str) -> int:
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
