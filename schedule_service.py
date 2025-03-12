from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import logging

class SchedulerService:
    def __init__(self):
        self.scheduler = BackgroundScheduler()
        self.scheduler.start()

    def create_schedule_job(self, job_class, job_name, job_group, cron_expression, job_data=None):
        """
        Tạo một công việc (job) theo lịch trình.
        :param job_class: Lớp công việc cần chạy.
        :param job_name: Tên công việc.
        :param job_group: Nhóm công việc.
        :param cron_expression: Biểu thức cron.
        :param job_data: Dữ liệu đi kèm với công việc.
        """
        logging.info(f"Start createScheduleJob: {job_name}")

        # Nếu job đã tồn tại, xóa nó trước
        self.delete_schedule_job(job_name, job_group)

        # Thêm job vào scheduler
        self.scheduler.add_job(
            job_class,
            trigger=CronTrigger.from_crontab(cron_expression),
            id=f"{job_group}.{job_name}",  # Định danh duy nhất cho job
            replace_existing=True,
            kwargs=job_data if job_data else {}
        )

        logging.info(f"End createScheduleJob: {job_name}")

    def delete_schedule_job(self, job_name, job_group=None):
        """
        Xóa công việc theo tên và nhóm.
        :param job_name: Tên công việc cần xóa.
        :param job_group: Nhóm công việc (nếu có).
        """
        job_id = f"{job_group}.{job_name}" if job_group else job_name

        if self.scheduler.get_job(job_id):
            logging.info(f"Deleting job: {job_id}")
            self.scheduler.remove_job(job_id)
        else:
            logging.info(f"Job {job_id} không tồn tại.")

    def shutdown(self):
        """ Dừng tất cả các công việc và shutdown scheduler """
        logging.info("Shutting down scheduler...")
        self.scheduler.shutdown()

# Nếu chạy script, khởi tạo SchedulerService
if __name__ == '__main__':
    scheduler_service = SchedulerService()
    # Ví dụ tạo job: scheduler_service.create_schedule_job(SomeJobClass, "job1", "group1", "0 0 * * *")
