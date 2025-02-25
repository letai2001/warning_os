import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List
from jinja2 import Environment, FileSystemLoader
from dataclasses import dataclass

@dataclass
class Article:
    title: str
    name: str
    created_at: str
    content: str
    link: str

class MailService:
    def __init__(self, smtp_server: str, smtp_port: int, smtp_user: str, smtp_password: str):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password

        # Cấu hình Jinja2 để load template từ thư mục "templates"
        self.env = Environment(loader=FileSystemLoader('templates'))

    def send_email(
        self, to: str, subject: str, title: str, name: str,
        primary_content: str, link: str, table_name: str,
        articles: List[Article], size: int
    ):
        """Gửi email với danh sách bài viết"""
        try:
            # Load template email
            template = self.env.get_template("email_template_tracing.html")
            
            # Render template với các biến động
            html_content = template.render(
                title=title,
                name=name,
                primary_content=primary_content,
                link=link,
                table_name=table_name,
                articles=articles,
                size=size
            )

            # Tạo email
            msg = MIMEMultipart()
            msg["From"] = self.smtp_user
            msg["To"] = to
            msg["Subject"] = subject
            msg.attach(MIMEText(html_content, "html"))

            # Kết nối SMTP và gửi email
            with smtplib.SMTP_SSL(self.smtp_server, self.smtp_port) as server:
                server.login(self.smtp_user, self.smtp_password)
                server.sendmail(self.smtp_user, to, msg.as_string())

            print(f"✅ Email đã gửi thành công đến {to}")

        except Exception as e:
            print(f"❌ Lỗi khi gửi email: {e}")
