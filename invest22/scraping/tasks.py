# invest22/scraping/tasks.py
from celery import shared_task
from .views import scrape_data
import logging

logger = logging.getLogger(__name__)

@shared_task
def scheduled_scrape():
    try:
        scrape_data()
        logger.info("Scraping de dados concluído com sucesso")
        return "Atualização concluída com sucesso"
    except Exception as e:
        logger.error(f"Erro durante o scraping: {str(e)}")
        return f"Erro na atualização: {str(e)}"
