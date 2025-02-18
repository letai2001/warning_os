import pickle
from typing import List
from PostMongo import PostMongo

class QuartzMapper:
    @staticmethod
    def decode_to_post(messages: bytes) -> List[PostMongo]:
        # Chuyển đổi dữ liệu từ bytes thành danh sách các PostMongo bằng pickle
        data = pickle.loads(messages)
        
        # Giả sử data là một danh sách các từ điển, chuyển mỗi từ điển thành đối tượng PostMongo
        return [PostMongo(**post) for post in data]
