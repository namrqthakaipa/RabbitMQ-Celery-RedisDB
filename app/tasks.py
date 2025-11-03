from app.celery_config import celery_app
from PIL import Image, ImageFilter
import redis
import json
import os
from datetime import datetime, timedelta
import time

# Redis client for caching
redis_client = redis.from_url(
    os.getenv('REDIS_URL', 'redis://localhost:6379/0'),
    decode_responses=True
)

@celery_app.task(bind=True, name='app.tasks.process_image')
def process_image(self, image_path, operations):
    """
    Process image with various operations
    Operations: resize, blur, grayscale, rotate
    """
    try:
        # Update task state
        self.update_state(state='PROGRESS', meta={'status': 'Loading image'})
        
        img = Image.open(image_path)
        
        # Apply operations
        if 'resize' in operations:
            self.update_state(state='PROGRESS', meta={'status': 'Resizing'})
            width, height = operations['resize']
            img = img.resize((width, height))
        
        if 'blur' in operations:
            self.update_state(state='PROGRESS', meta={'status': 'Applying blur'})
            img = img.filter(ImageFilter.BLUR)
        
        if 'grayscale' in operations:
            self.update_state(state='PROGRESS', meta={'status': 'Converting to grayscale'})
            img = img.convert('L')
        
        if 'rotate' in operations:
            self.update_state(state='PROGRESS', meta={'status': 'Rotating'})
            img = img.rotate(operations['rotate'])
        
        # Save processed image
        output_path = image_path.replace('uploads', 'processed')
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        img.save(output_path)
        
        # Cache result in Redis
        cache_key = f"processed_image:{os.path.basename(output_path)}"
        redis_client.setex(
            cache_key,
            3600,  # Expire in 1 hour
            json.dumps({
                'path': output_path,
                'operations': operations,
                'processed_at': datetime.now().isoformat()
            })
        )
        
        return {
            'status': 'completed',
            'output_path': output_path,
            'operations': operations
        }
    
    except Exception as e:
        return {'status': 'failed', 'error': str(e)}


@celery_app.task(name='app.tasks.send_email_notification')
def send_email_notification(recipient, subject, message):
    """
    Simulate sending email notification
    In production, use SendGrid, SES, or SMTP
    """
    time.sleep(2)  # Simulate email sending
    
    # Log to Redis
    log_key = f"email_log:{datetime.now().strftime('%Y-%m-%d')}"
    redis_client.lpush(log_key, json.dumps({
        'recipient': recipient,
        'subject': subject,
        'sent_at': datetime.now().isoformat()
    }))
    redis_client.expire(log_key, 86400 * 7)  # Keep for 7 days
    
    return {'status': 'sent', 'recipient': recipient}


@celery_app.task(name='app.tasks.cleanup_old_files')
def cleanup_old_files():
    """
    Periodic task to cleanup old processed files
    """
    processed_dir = 'processed'
    if not os.path.exists(processed_dir):
        return {'deleted': 0}
    
    deleted_count = 0
    cutoff_time = datetime.now() - timedelta(hours=24)
    
    for filename in os.listdir(processed_dir):
        filepath = os.path.join(processed_dir, filename)
        file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
        
        if file_time < cutoff_time:
            os.remove(filepath)
            deleted_count += 1
            # Remove from cache
            redis_client.delete(f"processed_image:{filename}")
    
    return {'deleted': deleted_count, 'timestamp': datetime.now().isoformat()}


@celery_app.task(name='app.tasks.generate_report')
def generate_report():
    """
    Generate daily statistics report
    """
    # Get all email logs from Redis
    today = datetime.now().strftime('%Y-%m-%d')
    log_key = f"email_log:{today}"
    
    email_count = redis_client.llen(log_key)
    
    # Store report in Redis
    report_key = f"daily_report:{today}"
    redis_client.setex(
        report_key,
        86400 * 30,  # Keep for 30 days
        json.dumps({
            'date': today,
            'emails_sent': email_count,
            'generated_at': datetime.now().isoformat()
        })
    )
    
    return {'date': today, 'emails_sent': email_count}


@celery_app.task(bind=True, name='app.tasks.long_running_task')
def long_running_task(self, duration=10):
    """
    Example of a long-running task with progress updates
    """
    for i in range(duration):
        time.sleep(1)
        self.update_state(
            state='PROGRESS',
            meta={
                'current': i + 1,
                'total': duration,
                'percent': int((i + 1) / duration * 100)
            }
        )
    
    return {'status': 'completed', 'duration': duration}
