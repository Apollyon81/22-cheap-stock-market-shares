from django.core.management.base import BaseCommand
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import os
from io import StringIO
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from structure.filters import apply_filters
from django.conf import settings
import json
from django.utils.timezone import now
from datetime import datetime, timedelta
from django.utils import timezone as dj_tz
import pytz
import requests
import time
import random
import logging

logger = logging.getLogger(__name__)




class Command(BaseCommand):
    help = 'Scrape the main table from Fundamentus, save raw and filtered'

    def handle(self, *args, **kwargs):
        url = "https://www.fundamentus.com.br/resultado.php"

        # Configuráveis por env vars
        max_attempts = int(os.environ.get("SCRAPE_HTTP_MAX_ATTEMPTS", "4"))
        base_backoff = float(os.environ.get("SCRAPE_HTTP_BACKOFF_BASE", "1.5"))
        max_backoff = float(os.environ.get("SCRAPE_HTTP_MAX_BACKOFF", "60"))
        jitter = float(os.environ.get("SCRAPE_HTTP_JITTER", "1.5"))

        # Etapa 0: checagem preliminar via requests para detectar 403/ban antes de abrir o webdriver
        session = requests.Session()
        session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
            "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,es;q=0.6",
            "Accept-Encoding": "gzip, deflate, br",
            "DNT": "1",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
            "Sec-Fetch-Dest": "document",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-Site": "none",
            "Sec-Fetch-User": "?1",
            "Cache-Control": "max-age=0",
        })

        attempt = 0
        allowed = False
        last_status = None
        while attempt < max_attempts:
            try:
                attempt += 1
                # Pequena pausa para simular comportamento humano
        time.sleep(2)
        r = session.get(url, timeout=15)
                last_status = r.status_code
                if r.status_code == 200:
                    allowed = True
                    break
                elif r.status_code == 403:
                    # se 403, log diagnóstico (opcional) e espera com backoff exponencial + jitter e tenta novamente
                    if os.environ.get('SCRAPE_VERBOSE_LOGGING') == '1':
                        try:
                            resp_headers = dict(r.headers) if getattr(r, 'headers', None) is not None else None
                            body = getattr(r, 'text', '')
                            if isinstance(body, str):
                                body = body[:1000].replace('\n', ' ').replace('\r', ' ')
                            logger.warning('Pre-scrape diagnostic: 403 detected (command). resp_headers=%s body_snip=%s', {k: resp_headers.get(k) for k in ['Server', 'X-Cache', 'Content-Type'] if resp_headers and k in resp_headers}, body)
                        except Exception:
                            logger.debug('Erro ao coletar dados de resposta para logging verboso (command)')
                    sleep_for = min(max_backoff, base_backoff * (2 ** (attempt - 1))) + random.uniform(0, jitter)
                    logger.warning("Pre-scrape check: recebido 403, tentativa %s/%s — dormindo %.1fs", attempt, max_attempts, sleep_for)
                    time.sleep(sleep_for)
                    continue
                else:
                    # para outros status 5xx/4xx, também tenta com backoff
                    sleep_for = min(max_backoff, base_backoff * (2 ** (attempt - 1))) + random.uniform(0, jitter)
                    logger.warning("Pre-scrape check: status %s, tentativa %s/%s — dormindo %.1fs", r.status_code, attempt, max_attempts, sleep_for)
                    time.sleep(sleep_for)
                    continue
            except Exception as e:
                sleep_for = min(max_backoff, base_backoff * (2 ** (attempt - 1))) + random.uniform(0, jitter)
                logger.warning("Pre-scrape check: erro na tentativa %s/%s: %s — dormindo %.1fs", attempt, max_attempts, e, sleep_for)
                time.sleep(sleep_for)

        if not allowed:
            # grava metadata com forbidden para evitar tentativas repetidas
            try:
                media_dir = os.path.join(settings.BASE_DIR, 'media')
                os.makedirs(media_dir, exist_ok=True)
                # Calcula backoff exponencial persistido via forbidden_count
                try:
                    existing = None
                    metadata_path = os.path.join(settings.BASE_DIR, "media", "metadata.json")
                    if os.path.exists(metadata_path):
                        try:
                            with open(metadata_path, 'r', encoding='utf-8') as f:
                                existing = json.load(f)
                        except Exception:
                            existing = None

                    existing_count = int(existing.get('forbidden_count', 0)) if isinstance(existing, dict) else 0
                    new_count = existing_count + 1

                    base_hours = int(os.environ.get("SCRAPE_BACKOFF_BASE_HOURS", "2"))
                    max_hours = int(os.environ.get("SCRAPE_BACKOFF_MAX_HOURS", "168"))
                    backoff_hours = min(base_hours * (2 ** (new_count - 1)), max_hours)
                    next_allowed = (now() + timedelta(hours=backoff_hours)).isoformat()

                    tz_sp = pytz.timezone('America/Sao_Paulo')
                    metadata = {
                        "last_scrape": now().isoformat(),
                        "last_scrape_local": now().astimezone(tz_sp).strftime("%d/%m/%Y %H:%M:%S %z"),
                        "last_attempt": now().isoformat(),
                        "last_attempt_local": now().astimezone(tz_sp).strftime("%d/%m/%Y %H:%M:%S %z"),
                        "next_allowed_attempt": next_allowed,
                        "next_allowed_attempt_local": (datetime.fromisoformat(next_allowed).astimezone(tz_sp).strftime("%d/%m/%Y %H:%M:%S %z")),
                        "status": "forbidden" if last_status == 403 else "error",
                        "http_status": last_status,
                        "forbidden_count": new_count,
                        "backoff_hours": backoff_hours,
                        "source_url": url
                    }

                    # Só grava se não houver um next_allowed_attempt futuro já presente
                    should_write = True
                    try:
                        if existing and existing.get('status') == 'forbidden' and existing.get('next_allowed_attempt'):
                            existing_next = datetime.fromisoformat(existing.get('next_allowed_attempt'))
                            if existing_next > now().astimezone(dj_tz.get_default_timezone()):
                                should_write = False
                    except Exception:
                        should_write = True

                    if should_write:
                        meta_tmp = metadata_path + '.tmp'
                        with open(meta_tmp, "w", encoding="utf-8") as f:
                            json.dump(metadata, f, ensure_ascii=False, indent=4)
                        os.replace(meta_tmp, metadata_path)
                        self.stdout.write(self.style.ERROR(f"Pre-check falhou (status={last_status}). metadata.json atualizado com status '{metadata['status']}' (backoff={backoff_hours}h)."))
                    else:
                        # Atualiza apenas forbidden_count se estiver em cooldown para aumentar backoff
                        try:
                            existing = existing or {}
                            existing['forbidden_count'] = new_count
                            with open(metadata_path + '.tmp', 'w', encoding='utf-8') as f:
                                json.dump(existing, f, ensure_ascii=False, indent=4)
                            os.replace(metadata_path + '.tmp', metadata_path)
                            self.stdout.write(self.style.WARNING("Pre-check falhou, mas já existe cooldown ativo — incrementado forbidden_count."))
                        except Exception:
                            self.stdout.write(self.style.WARNING("Pre-check falhou e não foi possível incrementar forbidden_count."))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"Falha ao calcular/gravar backoff metadata: {e}"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"Falha ao escrever metadata após pre-check: {e}"))
            return

        # Pequeno atraso aleatório antes de abrir o webdriver para dispersar solicitações
        time.sleep(random.uniform(0.5, 2.5))

        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        

        
        try:
            driver.get(url)

            table_el = WebDriverWait(driver, 20).until(
                EC.presence_of_element_located((By.ID, "resultado"))
            )

            table_html = table_el.get_attribute("outerHTML")

            # ============================================================
            # PASSO 1: Faz scraping e monta df_raw SEM ALTERAR NADA
            # ============================================================
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(table_html, "html.parser")
            rows = []

            for tr in soup.select("tr"):
                tds = [td.get_text(strip=True) for td in tr.find_all(["td", "th"])]
                if tds:
                    rows.append(tds)

            import pandas as pd
            df_raw = pd.DataFrame(rows[1:], columns=rows[0])
            
            # CRÍTICO: Converte todas as colunas para string para preservar formato
            for col in df_raw.columns:
                df_raw[col] = df_raw[col].apply(
                    lambda x: str(x) if pd.notna(x) and str(x) != 'nan' else ''
                )

            # Salva acoes_raw.csv exatamente como veio (sem alterações)
            raw_path = os.path.join(settings.BASE_DIR, 'media', 'acoes_raw.csv')
            os.makedirs(os.path.dirname(raw_path), exist_ok=True)
            raw_tmp = raw_path + '.tmp'
            df_raw.to_csv(raw_tmp, index=False, encoding='utf-8-sig')
            os.replace(raw_tmp, raw_path)
            self.stdout.write(self.style.SUCCESS(f"✔ acoes_raw.csv salvo: {raw_path}"))

            # ============================================================

            # Garante que lista_final é uma lista PLANA de strings
            lista_final = apply_filters(df_raw)

            # Achata a lista se vier como lista de listas/tuplas
            lista_final = [x[0] if isinstance(x, (list, tuple)) else x for x in lista_final]

            # Converte tudo para string
            lista_final = [str(x).strip() for x in lista_final]

            # ============================================================
            # PASSO 3: Montagem final - reabre raw, seleciona e reordena
            # ============================================================
            # Reabre acoes_raw.csv (garante que está lendo dados originais)
            df_raw_reload = pd.read_csv(
                raw_path,
                encoding='utf-8-sig',
                dtype=str  # Mantém tudo como string
            )

            # Seleciona apenas os tickers da lista_final
            df_final = df_raw_reload[df_raw_reload['Papel'].isin(lista_final)]

            # Reordena exatamente na ordem da lista_final
            df_final = df_final.set_index('Papel').loc[lista_final].reset_index()

            # ---- FILTRO DE COLUNAS AQUI ----
            colunas_finais = ['Papel', 'Liq.2meses', 'Mrg Ebit', 'EV/EBIT', 'P/L']
            df_final = df_final[colunas_finais]

            # Salva acoes_filtradas.csv (sem alterar conteúdo, apenas seleção e ordem)
            final_path = os.path.join(settings.BASE_DIR, 'media', 'acoes_filtradas.csv')
            final_tmp = final_path + '.tmp'
            df_final.to_csv(final_tmp, index=False, encoding='utf-8-sig')
            os.replace(final_tmp, final_path)
            self.stdout.write(self.style.SUCCESS("✔ acoes_filtradas.csv salvo."))

            # ============================================================
            # PASSO 4 → METADATA (agora está no local correto)
            # ============================================================
            tz_sp = pytz.timezone('America/Sao_Paulo')
            metadata = {
                "last_scrape": now().isoformat(),
                "last_scrape_local": now().astimezone(tz_sp).strftime("%d/%m/%Y %H:%M:%S %z"),
                "rows_raw": len(df_raw),
                "rows_filtered": len(df_final),
                "source_url": url,
                "status": "success"
            }

            metadata_path = os.path.join(settings.BASE_DIR, "media", "metadata.json")
            meta_tmp = metadata_path + '.tmp'
            with open(meta_tmp, "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            os.replace(meta_tmp, metadata_path)

            self.stdout.write(self.style.SUCCESS("✔ metadata.json salvo."))

            # PASSO 5: UPLOAD PARA S3 (se configurado)
            bucket = os.environ.get('AWS_S3_BUCKET')
            if bucket:
                try:
                    from structure.s3_utils import upload_file

                    # Upload dos arquivos gerados
                    upload_file(final_path, bucket, 'acoes_filtradas.csv')
                    upload_file(metadata_path, bucket, 'metadata.json')
                    upload_file(raw_path, bucket, 'acoes_raw.csv')

                    self.stdout.write(self.style.SUCCESS(f"✔ Arquivos enviados para S3: s3://{bucket}/"))
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f"⚠️ Erro no upload S3: {e}"))

            # Se S3 estiver configurado, faça upload dos artefatos para o bucket
            bucket = os.environ.get('AWS_S3_BUCKET')
            if bucket:
                try:
                    from structure.s3_utils import upload_file
                    # Upload raw, filtered e metadata
                    upload_file(raw_path, bucket, 'acoes_raw.csv')
                    upload_file(final_path, bucket, 'acoes_filtradas.csv')
                    upload_file(metadata_path, bucket, 'metadata.json')
                    self.stdout.write(self.style.SUCCESS(f"✔ artifacts uploaded to s3://{bucket}/"))
                except Exception as e:
                    self.stdout.write(self.style.WARNING(f"Warning: upload to S3 failed: {e}"))


        except Exception as e:
            # Em caso de erro, grava metadata com status de erro para facilitar debug
            try:
                metadata = {
                    "last_scrape": now().isoformat(),
                    "rows_raw": 0,
                    "rows_filtered": 0,
                    "source_url": url,
                    "status": "error",
                    "error": str(e)
                }
                metadata_path = os.path.join(settings.BASE_DIR, "media", "metadata.json")
                meta_tmp = metadata_path + '.tmp'
                os.makedirs(os.path.dirname(metadata_path), exist_ok=True)
                with open(meta_tmp, "w", encoding="utf-8") as f:
                    json.dump(metadata, f, ensure_ascii=False, indent=4)
                os.replace(meta_tmp, metadata_path)
                self.stdout.write(self.style.ERROR(f"Erro durante scraping: {e}. metadata.json atualizado com erro."))
            except Exception:
                # se falhar ao salvar metadata, apenas logamos
                self.stdout.write(self.style.ERROR(f"Erro durante scraping e falha ao gravar metadata: {e}"))
            finally:
                driver.quit()
            return
        finally:
            try:
                driver.quit()
            except Exception:
                pass
