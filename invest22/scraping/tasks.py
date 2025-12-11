# invest22/scraping/tasks.py
from celery import shared_task
import os
from django.conf import settings
import logging
import json
from django.utils.timezone import now
from datetime import datetime

logger = logging.getLogger(__name__)


@shared_task
def scheduled_scrape():
    """Executa scraping apenas se o último scraping não for do dia atual.

    O Celery Beat agenda esta task às 18:00. Aqui verificamos `media/metadata.json`.
    Se `last_scrape` já for de hoje, não executamos novamente.
    """
    try:
        metadata_path = os.path.join(settings.BASE_DIR, 'media', 'metadata.json')

        # Se existir metadata, verifica a data do último scraping
        if os.path.exists(metadata_path):
            try:
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    meta = json.load(f)
                last_scrape = meta.get('last_scrape')
                if last_scrape:
                    try:
                        last_dt = datetime.fromisoformat(last_scrape)
                        if last_dt.date() >= now().date():
                            logger.info('Scraping já realizado hoje (%s). Pule executação.', last_scrape)
                            return 'Já atualizado hoje'
                    except Exception:
                        # se parse falhar, prossegue com a execução
                        logger.warning('Não foi possível parsear last_scrape, prosseguindo com scraping')
            except Exception:
                logger.warning('Falha ao ler metadata.json — prosseguindo com scraping')

        # Importa o comando de scraping aqui para evitar import durante carga do web (se celery não estiver presente)
        try:
            from structure.management.commands.scrape_data import Command as ScrapeCommand
        except Exception as e:
            logger.error('Não foi possível importar ScrapeCommand: %s', e)
            return f'Erro: import scraper: {e}'

        # Executa o scraping (o comando grava acoes_raw.csv, acoes_filtradas.csv e metadata.json)
        scraper = ScrapeCommand()
        scraper.handle()

        # Verifica se metadata foi atualizada com sucesso
        if os.path.exists(metadata_path):
            try:
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    meta = json.load(f)
                last_scrape = meta.get('last_scrape')
                logger.info('Scraping concluído. metadata.last_scrape=%s', last_scrape)
            except Exception:
                logger.warning('Scraping finalizado, mas não foi possível ler metadata.json')

        return 'Atualização executada'

    except Exception as e:
        logger.exception('Erro durante a task scheduled_scrape:')
        return f'Erro na atualização: {e}'
