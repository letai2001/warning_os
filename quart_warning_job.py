import logging
import json
from QuartzMapper import QuartzMapper
from PeriodicWarningFacadeService import PeriodicWarningFacadeService

# Định nghĩa Job để APScheduler có thể gọi
class QuartWarningJob:
    def __init__(self):
        self.periodic_warning_service = PeriodicWarningFacadeService()  # Dịch vụ xử lý cảnh báo định kỳ

    def execute(self, **kwargs):
        """
        Hàm này tương đương với `executeInternal` trong Java.
        Nó sẽ lấy dữ liệu từ job, giải mã JSON và xử lý cảnh báo định kỳ.
        """
        logging.info("Job start executeInternal")

        # Lấy dữ liệu từ kwargs
        job_data = kwargs.get("job_data", {})

        # Chuyển đổi JSON thành đối tượng Warning
        if isinstance(job_data, str):
            job_data = json.loads(job_data)  # Nếu dữ liệu là string, giải mã JSON

        warning = QuartzMapper.json_to_warning(job_data.get("WARING_JOB_DATA_KEY", "{}"))

        # Gọi service để xử lý cảnh báo
        self.periodic_warning_service.handle_periodic_warning_v2(warning)

        logging.info("Job end executeInternal")
