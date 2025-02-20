from typing import List, Optional
from dataclasses import dataclass
from Reactionclass import Reaction

@dataclass
class PostMongo:
    id: str
    type: str
    time_crawl: int
    link: str
    author: str
    author_link: str
    author_id: str
    avatar: str
    created_time: int
    content: str
    image_url: Optional[List[str]] = None
    interactive: Optional[int] = 0
    like: Optional[int] = 0
    comment: Optional[int] = 0
    haha: Optional[int] = 0
    love: Optional[int] = 0
    wow: Optional[int] = 0
    sad: Optional[int] = 0
    angry: Optional[int] = 0
    share: Optional[int] = 0
    domain: Optional[str] = ''
    hashtag: Optional[List[str]] = None
    video: Optional[str] = ''
    videos: Optional[List[str]] = None
    source_id: Optional[str] = ''
    is_shared: Optional[int] = 0
    link_share: Optional[str] = ''
    title: Optional[str] = ''
    post_classify: Optional[str] = ''
    evaluate: Optional[str] = ''
    keyword: Optional[List[str]] = None
    topic_id: Optional[List[str]] = None
    description: Optional[str] = ''
    duration: Optional[int] = 0
    music: Optional[str] = ''
    view: Optional[int] = 0
    id_share: Optional[int] = 0
    type_share: Optional[str] = ''
    care: Optional[int] = 0
    avatar_key: Optional[str] = ''
    link_key: Optional[str] = ''
    image_url_keys: Optional[List[str]] = None
    video_key: Optional[str] = ''
    video_keys: Optional[List[str]] = None
    id_encode: Optional[str] = ''
    feedback_id: Optional[str] = ''
    feedback_expansion_token: Optional[str] = ''
    depth_comment: Optional[str] = ''
    parent_id: Optional[str] = ''
    location: Optional[str] = ''
    location_link: Optional[str] = ''
    message: Optional[str] = ''
    reactions_points: Optional[str] = ''
    points: Optional[int] = 0
    reacted_time: Optional[int] = 0
    role: Optional[str] = ''
    list_like: Optional[List[Reaction]] = None
    list_angry: Optional[List[Reaction]] = None
    list_haha: Optional[List[Reaction]] = None
    list_love: Optional[List[Reaction]] = None
    list_wow: Optional[List[Reaction]] = None
    out_links: Optional[List[str]] = None
    version: Optional[int] = 0

    def to_dict(self):
        return {
            "id": self.id,
            "type": self.type,
            "time_crawl": self.time_crawl,
            "link": self.link,
            "author": self.author,
            "author_link": self.author_link,
            "author_id": self.author_id,
            "avatar": self.avatar,
            "created_time": self.created_time,
            "content": self.content,
            "image_url": self.image_url or [],
            "interactive": self.interactive,
            "like": self.like,
            "comment": self.comment,
            "haha": self.haha,
            "love": self.love,
            "wow": self.wow,
            "sad": self.sad,
            "angry": self.angry,
            "share": self.share,
            "domain": self.domain,
            "hashtag": self.hashtag or [],
            "video": self.video,
            "videos": self.videos or [],
            "source_id": self.source_id,
            "is_shared": self.is_shared,
            "link_share": self.link_share,
            "title": self.title,
            "post_classify": self.post_classify,
            "evaluate": self.evaluate,
            "keyword": self.keyword or [],
            "topic_id": self.topic_id or [],
            "description": self.description,
            "duration": self.duration,
            "music": self.music,
            "view": self.view,
            "id_share": self.id_share,
            "type_share": self.type_share,
            "care": self.care,
            "avatar_key": self.avatar_key,
            "link_key": self.link_key,
            "image_url_keys": self.image_url_keys or [],
            "video_key": self.video_key,
            "video_keys": self.video_keys or [],
            "id_encode": self.id_encode,
            "feedback_id": self.feedback_id,
            "feedback_expansion_token": self.feedback_expansion_token,
            "depth_comment": self.depth_comment,
            "parent_id": self.parent_id,
            "location": self.location,
            "location_link": self.location_link,
            "message": self.message,
            "reactions_points": self.reactions_points,
            "points": self.points,
            "reacted_time": self.reacted_time,
            "role": self.role,
            "list_like": [reaction.to_dict() for reaction in self.list_like or []],
            "list_angry": [reaction.to_dict() for reaction in self.list_angry or []],
            "list_haha": [reaction.to_dict() for reaction in self.list_haha or []],
            "list_love": [reaction.to_dict() for reaction in self.list_love or []],
            "list_wow": [reaction.to_dict() for reaction in self.list_wow or []],
            "out_links": self.out_links or [],
            "version": self.version
        }
