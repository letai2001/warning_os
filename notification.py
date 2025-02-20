import logging
from typing import Optional
from warning_service import WarningHistoryResponse
import requests
class NotificationClientService:
    def __init__(self, api_url: str):
        self.api_url = api_url

    def send_notification(self, topic: str, warning_history_response: WarningHistoryResponse):
        # Thực hiện gọi HTTP POST đến WebSocket hoặc API socket của bạn
        try:
            response = requests.post(f"{self.api_url}/api/v1/socket", json=warning_history_response.to_dict())
            return response.text
        except Exception as e:
            logging.error(f"(sendNotification) error: {e}")
            return None


class NotificationService:
    def __init__(self, notification_client_service: NotificationClientService):
        self.notification_client_service = notification_client_service

    def create_warning_notification(self, warning_msg_response, warning_history_response):
        self.send_notification(warning_msg_response, warning_history_response)

    def send_notification(self, warning_msg_response, warning_history_response):
        try:
            response = self.notification_client_service.send_notification(
                "/topic/warning",
                warning_history_response
            )
            return response
        except Exception as e:
            logging.error(f"Error sending notification: {e}")
            return None
