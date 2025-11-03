from flask import Blueprint, request, jsonify, send_file
from werkzeug.utils import secure_filename
from app.tasks import process_image, send_email_notification, long_running_task
from app.celery_config import celery_app
import redis
import os
import json
from dotenv import load_dotenv

load_dotenv()

main_bp = Blueprint('main', __name__)
redis_client = redis.from_url(
    os.getenv('REDIS_URL', 'redis://localhost:6379/0'),
    decode_responses=True
)

ALLOWED_EXTENSIONS = {'png', 'jpg', 'jpeg', 'gif'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


@main_bp.route('/')
def index():
    return jsonify({
        'message': 'Celery + RabbitMQ + Redis API',
        'endpoints': {
            '/upload': 'POST - Upload and process image',
            '/task/<task_id>': 'GET - Check task status',
            '/send-email': 'POST - Send email notification',
            '/stats': 'GET - Get system statistics',
            '/long-task': 'POST - Start long running task'
        }
    })


@main_bp.route('/upload', methods=['POST'])
def upload_file():
    """Upload and process image"""
    if 'file' not in request.files:
        return jsonify({'error': 'No file provided'}), 400
    
    file = request.files['file']
    if file.filename == '' or not allowed_file(file.filename):
        return jsonify({'error': 'Invalid file'}), 400
    
    # Save file
    filename = secure_filename(file.filename)
    filepath = os.path.join('uploads', filename)
    file.save(filepath)
    
    # Get operations from request
    operations = {}
    if request.form.get('resize'):
        width, height = map(int, request.form.get('resize').split(','))
        operations['resize'] = (width, height)
    if request.form.get('blur'):
        operations['blur'] = True
    if request.form.get('grayscale'):
        operations['grayscale'] = True
    if request.form.get('rotate'):
        operations['rotate'] = int(request.form.get('rotate'))
    
    # Queue task
    task = process_image.delay(filepath, operations)
    
    # Send notification
    if request.form.get('email'):
        send_email_notification.delay(
            request.form.get('email'),
            'Image Processing Started',
            f'Your image {filename} is being processed'
        )
    
    return jsonify({
        'task_id': task.id,
        'status': 'queued',
        'message': 'Image processing started'
    })


@main_bp.route('/task/<task_id>')
def get_task_status(task_id):
    """Check task status"""
    task = celery_app.AsyncResult(task_id)
    
    response = {
        'task_id': task_id,
        'state': task.state,
    }
    
    if task.state == 'PENDING':
        response['status'] = 'Task is waiting to be executed'
    elif task.state == 'PROGRESS':
        response['status'] = task.info.get('status', '')
        response.update(task.info)
    elif task.state == 'SUCCESS':
        response['result'] = task.result
    elif task.state == 'FAILURE':
        response['error'] = str(task.info)
    
    return jsonify(response)


@main_bp.route('/send-email', methods=['POST'])
def send_email():
    """Send email notification"""
    data = request.get_json()
    
    task = send_email_notification.delay(
        data.get('recipient'),
        data.get('subject'),
        data.get('message')
    )
    
    return jsonify({
        'task_id': task.id,
        'status': 'queued'
    })


@main_bp.route('/long-task', methods=['POST'])
def start_long_task():
    """Start long running task"""
    data = request.get_json() or {}
    duration = data.get('duration', 10)
    
    task = long_running_task.delay(duration)
    
    return jsonify({
        'task_id': task.id,
        'status': 'started',
        'duration': duration
    })


@main_bp.route('/stats')
def get_stats():
    """Get system statistics from Redis"""
    # Get today's report
    today = redis_client.get(f"daily_report:{__import__('datetime').datetime.now().strftime('%Y-%m-%d')}")
    
    # Get active tasks count
    inspect = celery_app.control.inspect()
    active_tasks = inspect.active()
    
    return jsonify({
        'daily_report': json.loads(today) if today else None,
        'active_tasks': len(active_tasks.get('celery@worker', [])) if active_tasks else 0,
        'redis_keys': redis_client.dbsize()
    })
