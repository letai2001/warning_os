import pickle
from typing import List
from PostMongo import PostMongo
from warning_service import WarningMsgRequest
from elasticsearch import Elasticsearch
import json

class CommentESService:
    def __init__(self, es_host: str):
        # Khởi tạo client Elasticsearch
        self.client = Elasticsearch([es_host])
    
    def count_author_discussion(self, post_id: str) -> int:
        # Định nghĩa tên chỉ mục và trường (source_id_field)
        comment_collection_name = "comments"  # Thay thế bằng tên thực của collection bình luận
        source_id_field = "_id"  # Thay thế bằng tên trường thực

        # Tạo truy vấn để tìm kiếm tài liệu khớp với postId
        query = {
            "query": {
                "term": {
                    source_id_field: post_id
                }
            }
        }

        try:
            # Thực hiện truy vấn đếm tài liệu sử dụng Elasticsearch
            response = self.client.count(index=comment_collection_name, body=query)
            return response['count']  # Trả về số lượng tài liệu khớp
        except Exception as e:
            print(f"Đã có lỗi khi đếm bình luận cho bài đăng {post_id}: {e}")
            return 0  # Trả về 0 nếu có lỗi xảy ra

class QuartzMapper:
    @staticmethod
    def decode_to_post(messages: bytes) -> List[PostMongo]:
        # Chuyển đổi dữ liệu từ bytes thành danh sách các PostMongo bằng pickle
        data = pickle.loads(messages)
        
        # Giả sử data là một danh sách các từ điển, chuyển mỗi từ điển thành đối tượng PostMongo
        return [PostMongo(**post) for post in data]
    @staticmethod
    def build_warning_msg_request(warning_id: str, warning_history_id: str, post: PostMongo) -> WarningMsgRequest:
        content_post = post.title if post.title else post.content
        commentESService = CommentESService(es_host='http://172.168.200.202:9200')
        return WarningMsgRequest(
            post_id=post.id,
            warning_id=warning_id,
            warning_his_id=warning_history_id,
            channel=post.type,
            content_post=content_post,
            interactive_amount=post.interactive,
            discussion_amount=post.comment + post.share + 1,
            author_discussion_amount=commentESService.count_author_discussion(post.id),  
            like_amount=post.like + post.haha + post.sad + post.wow + post.angry + post.love,
            comment_amount=post.comment,
            share_amount=post.share,
            post_link=post.link,
            author_link=post.author_link
        )
    @staticmethod
    def decode_to_warning(messages: bytes) -> List[Warning]:
        """
        Giải mã dữ liệu từ bytes thành danh sách đối tượng Warning.
        :param messages: Dữ liệu bytes từ Kafka.
        :return: Danh sách các đối tượng Warning.
        """
        try:
            data = json.loads(messages.decode("utf-8")) 
            return [Warning(**warning) for warning in data]  
        except json.JSONDecodeError as e:
            print(f"Lỗi khi giải mã JSON: {e}")
            return []
    @staticmethod
    def json_to_warning(json_warning: str) -> Warning:
        """
        Chuyển đổi chuỗi JSON thành đối tượng Warning.
        :param json_warning: Chuỗi JSON cần chuyển đổi.
        :return: Đối tượng Warning hoặc None nếu có lỗi.
        """
        try:
            warning_data = json.loads(json_warning)  # Giải mã JSON
            return Warning(**warning_data)  # Chuyển dict thành object Warning
        except json.JSONDecodeError as e:
            print(f"Lỗi khi giải mã JSON thành Warning: {e}")
            return None
