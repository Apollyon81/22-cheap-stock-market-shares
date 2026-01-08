# invest22/scraping/tasks.py
# Importação opcional do Celery para evitar erro se não estiver instalado
try:
    from celery import shared_task
except (ImportError, ModuleNotFoundError):
    # Se Celery não estiver disponível, cria um decorator dummy
    def shared_task(func):
        return func

import os
from django.conf import settings
import logging
import json
from django.utils.timezone import now
from datetime import datetime, timedelta
import pytz

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

                # Se metadata indicar que o site bloqueou (forbidden), respeita cooldown
                status = meta.get('status')
                next_allowed = meta.get('next_allowed_attempt')
                if status == 'forbidden' and next_allowed:
                    try:
                        next_dt = datetime.fromisoformat(next_allowed)
                        next_dt_sp = next_dt.astimezone(pytz.timezone('America/Sao_Paulo'))
                        now_sp = now().astimezone(pytz.timezone('America/Sao_Paulo'))
                        if next_dt_sp > now_sp:
                            forbidden_count = meta.get('forbidden_count') if isinstance(meta, dict) else None
                            # Log com hora local de São Paulo
                            import pytz
                            now_sp = now().astimezone(pytz.timezone('America/Sao_Paulo')).strftime("%d/%m/%Y %H:%M:%S %z")
                            logger.info('Site bloqueado recentemente (status=forbidden). Pulando execução (cooldown). forbidden_count=%s next_allowed=%s now_sp=%s', forbidden_count, next_allowed, now_sp)
                            return 'Site bloqueado - cooldown'
                    except Exception:
                        logger.warning('Não foi possível parsear next_allowed_attempt, prosseguindo com scraping')

                last_scrape = meta.get('last_scrape')
                if last_scrape:
                    try:
                        last_dt = datetime.fromisoformat(last_scrape)
                        # Converter last_dt para timezone de São Paulo antes de comparar
                        last_dt_sp = last_dt.astimezone(pytz.timezone('America/Sao_Paulo'))
                        hoje_sp = now().astimezone(pytz.timezone('America/Sao_Paulo')).date()
                        if last_dt_sp.date() >= hoje_sp:
                            logger.info('Scraping já realizado hoje (%s). Pule execução.', last_scrape)
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
        # Mesmo em caso de erro, tenta escrever metadata básica para debugging
        try:
            metadata_error = {
                "last_scrape": now().isoformat(),
                "error": str(e),
                "status": "error"
            }
            metadata_path = os.path.join(settings.BASE_DIR, "media", "metadata.json")
            with open(metadata_path, "w", encoding="utf-8") as f:
                json.dump(metadata_error, f, ensure_ascii=False, indent=4)
        except:
            pass
        return f'Erro na atualização: {e}'
