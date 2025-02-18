from typing import List
from dataclasses import dataclass

@dataclass
class Reaction:
    author: str
    id: str
    role: str
    author_link: str
    avatar: str
    location: str
    location_link: str
    messages: str
    reactions_points: str
    points: int
    reacted_time: int

    def to_dict(self):
        return {
            "author": self.author,
            "id": self.id,
            "role": self.role,
            "author_link": self.author_link,
            "avatar": self.avatar,
            "location": self.location,
            "location_link": self.location_link,
            "messages": self.messages,
            "reactions_points": self.reactions_points,
            "points": self.points,
            "reacted_time": self.reacted_time
        }
