from django.shortcuts import render
import requests
from bs4 import BeautifulSoup
import pandas as pd
import os
from django.conf import settings
import logging
from django.utils.timezone import now
from datetime import datetime
import json
from django.utils import timezone as dj_tz

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


def _fetch_table_from_site(url: str):
    """Tenta obter a tabela do site usando headers de navegador e retries.

    Levanta requests.HTTPError em caso de resposta ruim (403, 500, etc.)
    ou ValueError se não encontrar a tabela.
    """
    session = requests.Session()
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://www.google.com/",
        "Connection": "keep-alive",
    }
    session.headers.update(headers)

    retries = Retry(total=2, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("https://", adapter)

    r = session.get(url, timeout=15)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    # Preferir o elemento com id 'resultado' (mesma referência do scraper anterior)
    table_el = soup.find(id="resultado")
    table = table_el if table_el is not None else soup.find("table")
    if table is None:
        raise ValueError("Tabela não encontrada no HTML")

    df = pd.read_html(str(table))[0]
    return df


def _read_cached_table():
    """Lê `media/acoes_filtradas.csv` e `media/metadata.json` como fallback."""
    media_dir = os.path.join(settings.BASE_DIR, "media")
    final_path = os.path.join(media_dir, "acoes_filtradas.csv")
    metadata_path = os.path.join(media_dir, "metadata.json")

    tabela_html = None
    data_atual = None

    if os.path.exists(final_path):
        try:
            df_final = pd.read_csv(final_path, encoding="utf-8-sig", dtype=str)
            tabela_html = df_final.to_html(classes="table table-striped", index=False, border=0)
        except Exception as e:
            logger.warning("Falha ao ler acoes_filtradas.csv: %s", e)

    if os.path.exists(metadata_path):
        try:
            with open(metadata_path, "r", encoding="utf-8") as f:
                meta = json.load(f)
            last = meta.get("last_scrape")
            if last:
                try:
                    last_dt = datetime.fromisoformat(last)
                    last_sp = last_dt.astimezone(dj_tz.get_default_timezone())
                    data_atual = last_sp.strftime("%d/%m/%Y %H:%M")
                except Exception:
                    data_atual = last
        except Exception as e:
            logger.warning("Falha ao ler metadata.json: %s", e)

    return tabela_html, data_atual


def home(request):
    url = "https://www.fundamentus.com.br/resultado.php"

    # Tenta primeiro buscar no site
    try:
        df = _fetch_table_from_site(url)
        # Aplicar o filtro mínimo (mesma regra anterior)
        if "Liquidez" in df.columns:
            try:
                # converter pra numérico, tratar separadores se necessário
                df["Liquidez"] = (
                    df["Liquidez"].astype(str).str.replace("\.", "", regex=False).str.replace(",", ".", regex=False)
                ).astype(float)
                df = df[df["Liquidez"] > 1_000_000]
            except Exception:
                # Se falhar na conversão, não aplica filtro
                logger.debug("Falha ao converter Liquidez — pulando filtro de liquidez")

        tabela_html = df.to_html(classes="table table-striped", index=False, border=0)
        data_atual = now().astimezone(dj_tz.get_default_timezone()).strftime("%d/%m/%Y %H:%M")

        # Opcional: atualizar metadata local para caching (não persiste se não desejado)
        try:
            media_dir = os.path.join(settings.BASE_DIR, "media")
            os.makedirs(media_dir, exist_ok=True)
            final_path = os.path.join(media_dir, "acoes_filtradas.csv")
            tmp = final_path + ".tmp"
            df.to_csv(tmp, index=False, encoding="utf-8-sig")
            os.replace(tmp, final_path)

            metadata = {
                "last_scrape": now().isoformat(),
                "rows_filtered": len(df),
                "source_url": url,
                "status": "success"
            }
            metadata_path = os.path.join(media_dir, "metadata.json")
            with open(metadata_path + ".tmp", "w", encoding="utf-8") as f:
                json.dump(metadata, f, ensure_ascii=False, indent=4)
            os.replace(metadata_path + ".tmp", metadata_path)
        except Exception as e:
            logger.warning("Falha ao gravar cache em media/: %s", e)

        return render(request, "structure/index.html", {"tabela_html": tabela_html, "data_atual": data_atual})

    except requests.HTTPError as e:
        status = getattr(e.response, "status_code", None)
        logger.warning("Erro HTTP ao buscar site: %s (status=%s)", e, status)
        # Fallback: ler cache local
        tabela_html, data_atual = _read_cached_table()
        if tabela_html is not None:
            # Adiciona nota que foi usado cache
            tabela_html = tabela_html + "<p><em>Dados carregados do cache local.</em></p>"
            if not data_atual:
                data_atual = now().astimezone(dj_tz.get_default_timezone()).strftime("%d/%m/%Y %H:%M")
            return render(request, "structure/index.html", {"tabela_html": tabela_html, "data_atual": data_atual})
        return render(request, "structure/index.html", {"tabela_html": f"<p>Erro: {e}</p>", "data_atual": None})

    except Exception as e:
        logger.exception("Erro ao buscar/parsear tabela:")
        tabela_html, data_atual = _read_cached_table()
        if tabela_html is not None:
            tabela_html = tabela_html + "<p><em>Dados carregados do cache local.</em></p>"
            return render(request, "structure/index.html", {"tabela_html": tabela_html, "data_atual": data_atual})
        return render(request, "structure/index.html", {"tabela_html": f"<p>Erro: {e}</p>", "data_atual": None})
