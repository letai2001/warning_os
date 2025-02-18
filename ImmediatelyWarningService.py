import logging
from typing import List, Dict
from datetime import datetime
from kafka import KafkaConsumer
from ThresholdHelper import ThresholdHelper
# from mail_service import MailService  # Giả sử bạn đã có thư viện gửi mail tương tự
# from notification_service import NotificationService  # Giả sử bạn có service này
# from quartz_mapper import QuartzMapper  # Giả sử bạn đã có QuartzMapper
# from user_service import UserService  # Giả sử bạn đã có service này
# from post_type_service import PostTypeService  # Service PostType
from warning_service import WarningService, WarningConditionService , WarningConditionResponse  ,ConditionObjectResponse# Service quản lý warning
from PostTypeService import PostTypeService, FilterPostTypeRequest, WarningUtils
# from warning_history_service import WarningHistoryService  # Service quản lý warning history
# from threshold_helper import ThresholdHelper  # Giả sử bạn có helper này để kiểm tra điều kiện cảnh báo
# from utils import WarningUtils  # Giả sử bạn có tiện ích để xử lý email và link
from PostMongo import PostMongo
class ImmediatelyWarningFacadeService:
    def __init__(self):
        self.warning_service = WarningService()
        self.warning_condition_service = WarningConditionService()  # Cần có class này
        # self.notification_service = NotificationService()
        self.post_type_service = PostTypeService()
        self.threshold_helper = ThresholdHelper()
        # self.mail_service = MailService()
        # self.topic_v2_service = TopicV2Service()  # Giả sử bạn có service này để lấy topic
        # self.warning_history_service = WarningHistoryService()
        # self.warning_msg_service = WarningMsgService()  # Giả sử có service gửi cảnh báo
        # self.quartz_mapper = QuartzMapper()
        # self.user_service = UserService()

    def handle_immediately_warning(self, post_mongo_list: List[PostMongo]):
        logging.info("(handle_immediately_warning) =========> START")

        active_immediately_warning_ids = self.warning_service.list_active_immediately_warning_ids()

        warning_id_to_condition_map = self.warning_condition_service.fetch_warning_conditions_by_warning_ids(active_immediately_warning_ids)

        for post_mongo in post_mongo_list:
            self.evaluate_post_against_warning_conditions(post_mongo, warning_id_to_condition_map)

    def evaluate_post_against_warning_conditions(self, post_mongo: PostMongo, warning_id_to_condition_map: Dict[str, List[WarningConditionResponse]]):
        all_post_types = self.post_type_service.filter(FilterPostTypeRequest(WarningUtils.get_full_post_type_key(), False))

        for warning_id, warning_conditions in warning_id_to_condition_map.items():
            for warning_condition in warning_conditions:
                condition_object = self.threshold_helper.get_condition_if_post_pass_through_warning_condition(post_mongo, warning_condition, all_post_types)

                if condition_object:
                    logging.info(f"(evaluate_post_against_warning_conditions) =========> postId : {post_mongo.id} THRESHOLD in condition: {condition_object}")
                    self.execute_warning_actions(warning_id, warning_condition, condition_object, post_mongo)
                    break

    def execute_warning_actions(self, warning_id: str, warning_condition: WarningConditionResponse, condition_object: ConditionObjectResponse, post_mongo: PostMongo):
        pass
    #     warning = self.warning_service.find(warning_id)
    #     topic_names = self.get_topic_names(warning_condition)

    #     keywords = warning_condition.get('keywords', [])

    #     warning_history = self.save_warning_history(warning_condition, condition_object.type, warning.created_by, topic_names, keywords, post_mongo.content, warning.methods)

    #     warning_message = self.save_warning_message(warning_id, warning_history.id, post_mongo)

    #     try:
    #         self.notification_service.create_warning_notification(warning_message, warning_history)
    #     except Exception as e:
    #         logging.error(e)

    #     if self.is_valid_user(warning.created_by):
    #         if WarningMethod.EMAIL in warning.methods:
    #             self.send_warning_to_email(warning_history.id, warning, warning_condition, condition_object.type, post_mongo, topic_names, keywords)

    # def send_warning_to_email(self, warning_history_id: str, warning: Warning, warning_condition: WarningConditionResponse, condition_type: int, post_mongo: PostMongo, topic_names: List[str], keywords: List[str]):
    #     logging.info(f"(send_warning_to_email) warningId: {warning.id}, postId: {post_mongo.id}")

    #     user = self.user_service.find_by_username(warning.created_by)

    #     warning_creation_user_email = user.email if user and user.email else None

    #     content_post = post_mongo.title if post_mongo.title else post_mongo.content

    #     subject = WarningUtils.build_subject_for_email_by_topic(topic_names, warning_condition.code) if warning_condition.criteria == WarningCriteria.TOPIC else WarningUtils.build_subject_for_email_by_keyword(keywords, warning_condition.code)

    #     primary_content = WarningUtils.build_warning_content_by_topic_immediately(topic_names, content_post) if warning_condition.criteria == WarningCriteria.TOPIC else WarningUtils.build_warning_content_by_keyword_immediately(keywords, content_post)

    #     link_warning = self.get_warning_link(warning_history_id)

    #     try:
    #         if warning_creation_user_email and warning.email != warning_creation_user_email:
    #             self.mail_service.send_email_v2(
    #                 warning_creation_user_email,
    #                 subject,
    #                 "Email Title",
    #                 "Blank",
    #                 primary_content,
    #                 link_warning,
    #                 "Table Topic",
    #                 [self.quartz_mapper.to_article(post_mongo)],
    #                 1
    #             )

    #         self.mail_service.send_email_v2(
    #             warning.email,
    #             subject,
    #             "Email Title",
    #             "Blank",
    #             primary_content,
    #             link_warning,
    #             "Table Topic",
    #             [self.quartz_mapper.to_article(post_mongo)],
    #             1
    #         )
    #     except Exception as e:
    #         logging.error(f"(send_warning_to_email) ==> Can not send warning to email: {e}")

    # def get_topic_names(self, warning_condition: WarningConditionResponse):
    #     topic_names = []
    #     if warning_condition.topics:
    #         topic_ids = [topic.id for topic in warning_condition.topics]
    #         topic_names = self.topic_v2_service.get_topic_names(topic_ids)
    #     return topic_names

    # def save_warning_history(self, warning_condition: WarningConditionResponse, condition_type: int, created_by: str, topic_names: List[str], keywords: List[str], primary_content: str, warning_method: List[WarningMethod]):
    #     title = TOPIC_CROSSED_THRESHOLD if warning_condition.criteria == WarningCriteria.TOPIC else KEYWORD_CROSSED_THRESHOLD
    #     title = f"{title}#{warning_condition.code.upper()}" if warning_condition.code else title

    #     content = WarningUtils.build_warning_content_by_topic_immediately(topic_names, primary_content) if warning_condition.criteria == WarningCriteria.TOPIC else WarningUtils.build_warning_content_by_keyword_immediately(keywords, primary_content)

    #     warning_history_request = WarningHistoryRequest(
    #         type=WarningType.UNEXPECTED.value,
    #         title=title,
    #         content=content,
    #         criteria=warning_condition.criteria,
    #         created_by=created_by,
    #         is_notification=WarningMethod.NOTIFICATION in warning_method
    #     )

    #     return self.warning_history_service.create(warning_history_request)

    # def save_warning_message(self, warning_id: str, warning_history_id: str, post_mongo: PostMongo):
    #     warning_msg_request = self.quartz_mapper.build_warning_msg_request(warning_id, warning_history_id, post_mongo)
    #     return self.warning_msg_service.create(warning_msg_request)

    # def is_valid_user(self, username: str) -> bool:
    #     user = self.user_service.find_by_username(username)
    #     return user and not user.is_deleted and user.status == ActivationStatus.ACTIVE.value
