import logging
from typing import List, Dict
from datetime import datetime
from kafka import KafkaConsumer
from ThresholdHelper import ThresholdHelper
from notification import NotificationService , NotificationClientService # Giả sử bạn có service này, NotificationClientService
from warning_service import WarningMsgRequest, WarningMsgResponse, WarningMsgService, WarningHistoryService , WarningHistoryRequest, WarningService, WarningConditionService , WarningConditionResponse  , ConditionObjectResponse, TopicV2Service, WarningHistoryResponse, WarningMethod, WarningCriteria, WarningType, Warning# Service quản lý warning
from PostTypeService import PostTypeService, FilterPostTypeRequest, WarningUtils
from QuartzMapper import QuartzMapper
from User import UserService
from PostMongo import PostMongo
from mail_service import MailService
list_warning_frontend_url = "http://192.168.143.183:3001/notification/detail/"

DB_URL = '172.168.200.202:30000'
class PeriodicWarningFacadeService:
    def __init__(self):
        self.warning_service = WarningService(db_url=DB_URL, db_name='osint')
        self.warning_condition_service = WarningConditionService(db_url=DB_URL, db_name='osint')  # Cần có class này
        # self.notification_service = NotificationService()
        self.post_type_service = PostTypeService(db_url=DB_URL, db_name='osint')
        self.threshold_helper = ThresholdHelper()
        # self.mail_service = MailService()
        self.notification_client = NotificationClientService(api_url="http://your-api-url.com")

# Khởi tạo NotificationService
        self.notification_service = NotificationService(notification_client_service=self.notification_client)

        self.topic_v2_service = TopicV2Service(db_url=DB_URL, db_name='osint') 
        self.warning_history_service = WarningHistoryService(db_url=DB_URL, db_name='osint')
        self.warning_msg_service = WarningMsgService(db_url=DB_URL, db_name='osint')  
        self.quartz_mapper = QuartzMapper()
        self.user_service = UserService()
        self.mail_service = MailService(
            smtp_server="smtp.gmail.com",
            smtp_port=587,
            sender_email="your-email@gmail.com",
            sender_password="your-password",
            template_path="templates"
        )
    def handle_periodic_warning_v2(self, warning: Warning):
        """
        Xử lý cảnh báo định kỳ.
        :param warning: Đối tượng cảnh báo.
        """
        logging.info(f"(handle_periodic_warning_v2) warning: {warning}")

        is_exceed_threshold = False
        conditions = self.warning_condition_service.find_by_warning_ids(warning.id)

        for condition in conditions:
            for condition_object in condition.conditionObjectResponses:
                crossed_threshold_object = None

                if condition.criteria == WarningCriteria.TOPIC:
                    crossed_threshold_object = self.threshold_helper.get_crossed_threshold_object_by_topic(
                        condition.threshold_time, condition.threshold_time_amount, condition.topics, condition_object
                    )
                elif condition.criteria == WarningCriteria.KEYWORD:
                    crossed_threshold_object = self.threshold_helper.get_crossed_threshold_object_by_keyword(
                        condition.threshold_time, condition.threshold_time_amount, condition.keywords, condition_object
                    )
                elif condition.criteria == WarningCriteria.OBJECT:
                    crossed_threshold_object = self.threshold_helper.get_crossed_threshold_object_by_object(
                        condition.threshold_time, condition.threshold_time_amount, condition_object
                    )

                if crossed_threshold_object:
                    logging.info("(handle_periodic_warning_v2) ==> CROSS THRESHOLD DETECTED")
                    self.execute_warning_actions(warning, condition, condition_object, crossed_threshold_object)
                    is_exceed_threshold = True
                    break

            if is_exceed_threshold:
                break
