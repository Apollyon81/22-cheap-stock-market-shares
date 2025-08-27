# invest22/celery.py
import os
from celery import Celery

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'invest22.settings')

app = Celery('invest22')
app.config_from_object('django.conf:settings', namespace='CELERY')
app.autodiscover_tasks()
