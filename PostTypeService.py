from pymongo import MongoClient
from typing import List

class PostTypeResponse:
    def __init__(self, id: str, name: str, key: str, is_comment: bool):
        self.id = id
        self.name = name
        self.key = key
        self.is_comment = is_comment

    def __repr__(self):
        return f"PostTypeResponse(id={self.id}, name={self.name}, key={self.key}, is_comment={self.is_comment})"
class FilterPostTypeRequest:
    def __init__(self, keys: List[str], is_comment: bool):
        self.keys = keys
        self.is_comment = is_comment

    @staticmethod
    def of(keys: List[str], is_comment: bool):
        return FilterPostTypeRequest(keys, is_comment)
class WarningUtils:
    PREFIX_CONTENT_IMMEDIATELY = ': "'
    BLANK = ''
    TOPIC = "Chủ đề "
    POST_THRESHOLD_EXCEEDED = " có bài viết vượt ngưỡng"

    @staticmethod
    def get_full_post_type_key():
        return ["facebook", "tiktok", "youtube", "electronic", "forums"]
    @staticmethod
    def limit_content_has_max_20_words(input: str) -> str:
        if input is None:
            return None
        words = input.split()
        if len(words) <= 20:
            return input
        else:
            truncated = ' '.join(words[:20])
            return truncated + "..."
    @staticmethod
    def build_warning_content_by_topic_immediately(topic_names: list, primary_content: str) -> str:
        if primary_content is None:
            primary_content = WarningUtils.BLANK
        else:
            primary_content = WarningUtils.limit_content_has_max_20_words(primary_content)
            primary_content = WarningUtils.PREFIX_CONTENT_IMMEDIATELY + primary_content + '"'

        result = ', '.join([f'"{topic_name}"' for topic_name in topic_names])

        return WarningUtils.TOPIC + result + WarningUtils.POST_THRESHOLD_EXCEEDED + primary_content
    @staticmethod

    def build_warning_content_by_keyword_immediately(keywords: list, primary_content: str) -> str:
        if primary_content is None:
            primary_content = WarningUtils.BLANK
        else:
            primary_content = WarningUtils.limit_content_has_max_20_words(primary_content)
            primary_content = WarningUtils.PREFIX_CONTENT_IMMEDIATELY + primary_content + '"'

        result = ', '.join([f'"{keyword}"' for keyword in keywords])

        return WarningUtils.KEYWORD + result + WarningUtils.POST_THRESHOLD_EXCEEDED + primary_content


class PostTypeService:
    def __init__(self, db_url: str, db_name: str):
        self.client = MongoClient(db_url)
        self.db = self.client[db_name]
        self.collection = self.db['post_types']

    def filter(self, request: FilterPostTypeRequest) -> List[PostTypeResponse]:
        criteria = {}
        if request.keys:
            criteria["key"] = {"$in": request.keys}
        if request.is_comment is not None:
            criteria["is_comment"] = request.is_comment
        
        # Lọc dữ liệu từ MongoDB
        result = self.collection.find(criteria, {"_id": 1, "name": 1, "key": 1, "is_comment": 1})
        return [PostTypeResponse(doc["_id"], doc["name"], doc["key"], doc["is_comment"]) for doc in result]
def main():
    # Tạo đối tượng PostTypeService và kiểm tra filter
    post_type_service = PostTypeService(db_url='10.11.32.22:30000', db_name='osint')
    all_post_types = post_type_service.filter(FilterPostTypeRequest(WarningUtils.get_full_post_type_key(), False))
    
    # In ra kết quả
    print("All Post Types:", all_post_types)

# Chạy hàm main
if __name__ == "__main__":
    main()
