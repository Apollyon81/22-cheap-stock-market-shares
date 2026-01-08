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
from structure.filters import apply_filters

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

    # Extrai o texto das células para preservar exatamente o formato original
    rows = []
    for tr in table.find_all('tr'):
        tds = [td.get_text(strip=True) for td in tr.find_all(['td', 'th'])]
        if tds:
            rows.append(tds)

    df = pd.DataFrame(rows[1:], columns=rows[0])

    # Garantir que todas as colunas sejam strings (preservar formatação BR como '9,99')
    for col in df.columns:
        df[col] = df[col].apply(lambda x: str(x) if pd.notna(x) and str(x) != 'nan' else '')
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

    # Checa metadata para evitar tentar fetch repetidas vezes quando o site bloqueia (403)
    media_dir = os.path.join(settings.BASE_DIR, "media")
    metadata_path = os.path.join(media_dir, "metadata.json")
    cooldown_hours = int(os.environ.get("SCRAPE_BLOCK_COOLDOWN_HOURS", "24"))
    try:
        if os.path.exists(metadata_path):
            with open(metadata_path, "r", encoding="utf-8") as f:
                meta_current = json.load(f)
            if meta_current.get("status") == "forbidden":
                last_attempt = meta_current.get("last_attempt") or meta_current.get("last_scrape")
                if last_attempt:
                    try:
                        last_dt = datetime.fromisoformat(last_attempt)
                        cutoff = now().astimezone(dj_tz.get_default_timezone()) - pd.Timedelta(hours=cooldown_hours)
                        if last_dt.astimezone(dj_tz.get_default_timezone()) > cutoff:
                            # Dentro do período de cooldown: usa cache imediatamente
                            tabela_html, data_atual = _read_cached_table()
                            if tabela_html is not None:
                                tabela_html = tabela_html + "<p><em>Dados carregados do cache local (site bloqueado).</em></p>"
                                return render(request, "structure/index.html", {"tabela_html": tabela_html, "data_atual": data_atual})
                    except Exception:
                        pass
    except Exception:
        # não bloqueia a execução se metadata estiver corrompido
        pass

    # Tenta primeiro buscar no site
    try:
        df = _fetch_table_from_site(url)
        # Salva raw (sem alterações) para referência e para aplicar filtros de forma consistente
        media_dir = os.path.join(settings.BASE_DIR, "media")
        try:
            os.makedirs(media_dir, exist_ok=True)
            raw_path = os.path.join(media_dir, "acoes_raw.csv")
            raw_tmp = raw_path + ".tmp"
            df.to_csv(raw_tmp, index=False, encoding="utf-8-sig")
            os.replace(raw_tmp, raw_path)
        except Exception as e:
            logger.warning("Falha ao salvar acoes_raw.csv: %s", e)

        tabela_html = None
        df_final = None
        try:
            # Usa a função de filtros compartilhada para gerar a lista de Papéis
            lista_final = apply_filters(df)
            lista_final = [x[0] if isinstance(x, (list, tuple)) else x for x in lista_final]
            lista_final = [str(x).strip() for x in lista_final]

            # Reabre o raw para garantir consistência e monta df_final
            df_raw_reload = pd.read_csv(raw_path, encoding="utf-8-sig", dtype=str)
            if 'Papel' in df_raw_reload.columns:
                df_final = df_raw_reload[df_raw_reload['Papel'].isin(lista_final)]
                df_final = df_final.set_index('Papel').loc[lista_final].reset_index()
                colunas_finais = ['Papel', 'Liq.2meses', 'Mrg Ebit', 'EV/EBIT', 'P/L']
                colunas_existentes = [c for c in colunas_finais if c in df_final.columns]
                df_final = df_final[colunas_existentes]
                tabela_html = df_final.to_html(classes="table table-striped", index=False, border=0)
        except Exception as e:
            logger.warning("Falha ao aplicar filtros: %s", e)

        # Se por algum motivo não foi possível montar a tabela filtrada, exibe raw
        if tabela_html is None:
            tabela_html = df.to_html(classes="table table-striped", index=False, border=0)

        data_atual = now().astimezone(dj_tz.get_default_timezone()).strftime("%d/%m/%Y %H:%M")

        # Salva resultado filtrado (se disponível) e metadata
        try:
            os.makedirs(media_dir, exist_ok=True)
            final_path = os.path.join(media_dir, "acoes_filtradas.csv")
            tmp = final_path + ".tmp"
            to_save = df_final if df_final is not None else df
            to_save.to_csv(tmp, index=False, encoding="utf-8-sig")
            os.replace(tmp, final_path)

            metadata = {
                "last_scrape": now().isoformat(),
                "rows_raw": len(df),
                "rows_filtered": len(to_save) if hasattr(to_save, 'shape') else 0,
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
        # Se for 403, grava metadata com status forbidden e last_attempt, para evitar loop de tentativas
        try:
            os.makedirs(media_dir, exist_ok=True)
            metadata_forbidden = {
                "last_scrape": meta_current.get("last_scrape") if 'meta_current' in locals() and isinstance(meta_current, dict) else None,
                "last_attempt": now().isoformat(),
                "status": "forbidden",
                "source_url": url,
            }
            with open(metadata_path + ".tmp", "w", encoding="utf-8") as f:
                json.dump(metadata_forbidden, f, ensure_ascii=False, indent=4)
            os.replace(metadata_path + ".tmp", metadata_path)
        except Exception:
            logger.warning("Não foi possível gravar metadata de forbidden")
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
