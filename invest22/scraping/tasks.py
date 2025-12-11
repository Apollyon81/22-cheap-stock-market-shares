# invest22/scraping/tasks.py
from celery import shared_task
import os
from django.conf import settings
from structure.management.commands.scrape_acoes import Command as ScrapeCommand
import logging

logger = logging.getLogger(__name__)

@shared_task
def scheduled_scrape():
    try:
        # Caminho do CSV
        csv_path = os.path.join(settings.BASE_DIR, 'media', 'acoes_filtradas.csv')
        
        # Apaga CSV antigo, se existir
        if os.path.exists(csv_path):
            os.remove(csv_path)
            logger.info("CSV antigo removido")

        # Executa o scraping
        scraper = ScrapeCommand()
        scraper.handle()  # roda o scraping e cria o novo CSV
        
        logger.info("Scraping de dados concluído com sucesso")
        return "Atualização concluída com sucesso"
    
    except Exception as e:
        logger.error(f"Erro durante o scraping: {str(e)}")
        return f"Erro na atualização: {str(e)}"
