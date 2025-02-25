from dataclasses import dataclass
from typing import List, Optional
from warning_service import BaseEntity 
from pymongo import MongoClient
import logging
class UserNotFoundException(Exception):
    """Ngoại lệ khi không tìm thấy User."""
    pass

@dataclass
class UserDepartmentRole:
    id: str
    role: str
    name: str

    def __eq__(self, other):
        """Xác định khi hai đối tượng UserDepartmentRole bằng nhau."""
        if isinstance(other, UserDepartmentRole):
            return self.id == other.id and self.role == other.role
        return False

    def __hash__(self):
        """Trả về mã băm để có thể sử dụng trong tập hợp (set) hoặc dictionary keys."""
        return hash((self.id, self.role))
@dataclass
class User(BaseEntity):
    username: str
    password: str
    email: str
    full_name: str
    phone: str
    role: str
    status: int
    departments: Optional[List[UserDepartmentRole]] = None
    topic_ids: Optional[List[str]] = None
    deleted: bool = False

    def __repr__(self):
        return (f"User(id={self.id}, username={self.username}, email={self.email}, "
                f"full_name={self.full_name}, phone={self.phone}, role={self.role}, "
                f"status={self.status}, deleted={self.deleted})")

    @staticmethod
    def from_dict(data: dict) -> "User":
        """Tạo một User từ dictionary dữ liệu lấy từ MongoDB"""
        return User(
            id=str(data.get("_id", "")),  
            username=data.get("username", ""),
            password=data.get("password", ""),
            email=data.get("email", ""),
            full_name=data.get("full_name", ""),
            phone=data.get("phone", ""),
            role=data.get("role", ""),
            status=data.get("status", 0),
            departments=[UserDepartmentRole(**dept) for dept in data.get("departments", [])] if "departments" in data else None,
            topic_ids=data.get("topic_ids", []),
            deleted=data.get("deleted", False),
            created_by=data.get("created_by", ""),
            created_at=data.get("created_at", 0),
            updated_by=data.get("updated_by", ""),
            updated_at=data.get("updated_at", 0)
        )
class UserService:
    def __init__(self, db_url: str, db_name: str):
        self.client = MongoClient(db_url)
        self.db = self.client[db_name]
        self.collection = self.db['users']  # Collection 'users' trong MongoDB

    def find_by_username(self, username: str) -> User:
        """Tìm kiếm User theo username (bỏ qua User đã bị xóa)."""
        logging.info("=== Start find user by username")
        logging.debug(f"=== Searching for user with username: {username}")

        user_data = self.collection.find_one({"username": username, "deleted": False})

        if not user_data:
            raise UserNotFoundException(f"User '{username}' not found!")

        return User(
            id=str(user_data["_id"]),
            username=user_data["username"],
            email=user_data.get("email"),
            deleted=user_data.get("deleted", False)
        )
