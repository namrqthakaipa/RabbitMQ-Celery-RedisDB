from celery import Celery
from celery.schedules import crontab
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize Celery
celery_app = Celery(
    'tasks',
    broker=os.getenv('RABBITMQ_URL', 'amqp://guest:guest@localhost:5672//'),
    backend=os.getenv('REDIS_URL', 'redis://localhost:6379/0')
)

# Celery Configuration
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutes
    result_expires=3600,  # 1 hour
)

# Periodic Tasks Configuration
celery_app.conf.beat_schedule = {
    'cleanup-old-files-every-hour': {
        'task': 'app.tasks.cleanup_old_files',
        'schedule': crontab(minute=0, hour='*/1'),  # Every hour
    },
    'generate-daily-report': {
        'task': 'app.tasks.generate_report',
        'schedule': crontab(hour=9, minute=0),  # Every day at 9 AM
    },
}
