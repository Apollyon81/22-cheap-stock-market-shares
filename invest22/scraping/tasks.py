# invest22/scraping/tasks.py
from celery import shared_task
from .views import scrape_data

@shared_task
def scheduled_scrape():
    scrape_data()
