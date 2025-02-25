from pymongo import MongoClient
from typing import List
import datetime
from mail_service import Article
from PostMongo import PostMongo
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
    KEYWORD = "Từ khóa "
    POST_THRESHOLD_EXCEEDED = " có bài viết vượt ngưỡng"
    TOPIC_THRESHOLD_EXCEEDED_WARNING = "[OSINT] Cảnh báo chủ đề vượt ngưỡng: "
    CONFIGURATION_WARNING_PREFIX = " theo cấu hình #"
    KEYWORD_THRESHOLD_EXCEEDED_WARNING = "[OSINT] Cảnh báo từ khóa vượt ngưỡng: "
    TYPE_WARNING_PARAM = "?type=0"
    HAS_NO_TITLE = "Không có tiêu đề"
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
    @staticmethod
    def build_subject_for_email_by_topic(topic_names: List[str], warning_condition_code: str) -> str:
        """
        Tạo tiêu đề email cho cảnh báo theo chủ đề.

        :param topic_names: Danh sách các tên chủ đề
        :param warning_condition_code: Mã điều kiện cảnh báo
        :return: Tiêu đề email
        """
        if not topic_names:
            return f"{WarningUtils.TOPIC_THRESHOLD_EXCEEDED_WARNING}Không có chủ đề{WarningUtils.CONFIGURATION_WARNING_PREFIX}{warning_condition_code.upper()}"

        # Nối các chủ đề thành chuỗi
        topic_str = ', '.join(f'"{topic}"' for topic in topic_names)

        return f"{WarningUtils.TOPIC_THRESHOLD_EXCEEDED_WARNING}{topic_str}{WarningUtils.CONFIGURATION_WARNING_PREFIX}{warning_condition_code.upper()}"

    @staticmethod
    def build_subject_for_email_by_keyword(keywords: List[str], warning_condition_code: str) -> str:
        """
        Tạo tiêu đề email cho cảnh báo theo từ khóa.

        :param keywords: Danh sách các từ khóa
        :param warning_condition_code: Mã điều kiện cảnh báo
        :return: Tiêu đề email
        """
        if not keywords:
            return f"{WarningUtils.KEYWORD_THRESHOLD_EXCEEDED_WARNING}Không có từ khóa{WarningUtils.CONFIGURATION_WARNING_PREFIX}{warning_condition_code.upper()}"

        # Nối các từ khóa thành chuỗi
        keyword_str = ', '.join(f'"{keyword}"' for keyword in keywords)

        return f"{WarningUtils.KEYWORD_THRESHOLD_EXCEEDED_WARNING}{keyword_str}{WarningUtils.CONFIGURATION_WARNING_PREFIX}{warning_condition_code.upper()}"
    @staticmethod
    def build_link_for_email(warningId: str) -> str:
        """
        Tạo link cho email cảnh báo.

        :param post_link: Link bài viết
        :param author_link: Link tác giả
        :return: Chuỗi HTML chứa link
        """
        return warningId + WarningUtils.TYPE_WARNING_PARAM 
    @staticmethod
    def format_timestamp(timestamp: int) -> str:
        """Chuyển đổi timestamp sang định dạng datetime (YYYY-MM-DD HH:MM:SS)"""
        return datetime.utcfromtimestamp(timestamp / 1000).strftime('%Y-%m-%d %H:%M:%S')
    @staticmethod
    def to_article(post_mongo: PostMongo) -> Article:
        """Chuyển đổi `PostMongo` thành `Article`"""
        title = WarningUtils.HAS_NO_TITLE
        content = post_mongo.content

        if post_mongo.title and post_mongo.title.strip():
            title = WarningUtils.limit_content_has_max_20_words(post_mongo.title)

        if post_mongo.content and post_mongo.content.strip():
            content = WarningUtils.limit_content_has_max_20_words(post_mongo.content)

        return Article(
            title=title,
            name=post_mongo.author,
            content=content,
            created_at=WarningUtils.format_timestamp(post_mongo.created_time),
            link=post_mongo.link
        )

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
