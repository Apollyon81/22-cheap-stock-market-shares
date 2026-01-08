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
from structure.filters import clean_numeric

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

    # Se S3 estiver configurado, tente ler do S3 primeiro
    bucket = os.environ.get('AWS_S3_BUCKET')
    if bucket:
        try:
            from structure.s3_utils import get_csv_df, get_json
            csv_io = get_csv_df(bucket, 'acoes_filtradas.csv')
            df_final = pd.read_csv(csv_io, encoding='utf-8-sig', dtype=str)
            tabela_html = df_final.to_html(classes="table table-striped", index=False, border=0)

            meta = get_json(bucket, 'metadata.json')
            last = meta.get('last_scrape')
            if last:
                try:
                    last_dt = datetime.fromisoformat(last)
                    last_sp = last_dt.astimezone(dj_tz.get_default_timezone())
                    data_atual = last_sp.strftime("%d/%m/%Y %H:%M")
                except Exception:
                    data_atual = last
            return tabela_html, data_atual
        except Exception:
            logger.warning("Falha ao ler arquivos do S3 — fallback para local")

    if os.path.exists(final_path):
        try:
            df_final = pd.read_csv(final_path, encoding="utf-8-sig", dtype=str)
            # Formata colunas numéricas para exibição (BR format)
            df_display = _format_display_df(df_final)
            tabela_html = df_display.to_html(classes="table table-striped", index=False, border=0)
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
    # Caso metadata esteja ausente, tenta inferir última modificação do arquivo
    if data_atual is None and os.path.exists(final_path):
        try:
            mtime = os.path.getmtime(final_path)
            dt = datetime.fromtimestamp(mtime, tz=dj_tz.get_default_timezone())
            data_atual = dt.strftime("%d/%m/%Y %H:%M")
        except Exception:
            pass

    return tabela_html, data_atual


def home(request):
    url = "https://www.fundamentus.com.br/resultado.php"

    # Checa metadata para evitar fetch repetidas vezes quando o site bloqueia (403)
    media_dir = os.path.join(settings.BASE_DIR, "media")
    metadata_path = os.path.join(media_dir, "metadata.json")
    cooldown_hours = int(os.environ.get("SCRAPE_BLOCK_COOLDOWN_HOURS", "24"))
    try:
        if os.path.exists(metadata_path):
            with open(metadata_path, "r", encoding="utf-8") as f:
                meta_current = json.load(f)

            # Preferimos um campo explícito `next_allowed_attempt` (ISO).
            # Se presente e no futuro, não tentamos buscar no site.
            next_allowed = meta_current.get("next_allowed_attempt")
            if meta_current.get("status") == "forbidden" and next_allowed:
                try:
                    next_dt = datetime.fromisoformat(next_allowed)
                    now_dt = now().astimezone(dj_tz.get_default_timezone())
                    if next_dt.astimezone(dj_tz.get_default_timezone()) > now_dt:
                        # Ainda em cooldown → usa cache imediatamente
                        tabela_html, data_atual = _read_cached_table()
                        if tabela_html is not None:
                            nota = "Dados carregados do cache local (site bloqueado)."
                            if data_atual:
                                nota = f"{nota} Última atualização: {data_atual}."
                            tabela_html = tabela_html + f"<p><em>{nota}</em></p>"
                            return render(request, "structure/index.html", {"tabela_html": tabela_html, "data_atual": data_atual})
                except Exception:
                    # se parse falhar, caímos para o comportamento anterior (tentar buscar)
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
                # Formata para exibição (BR format)
                df_display = _format_display_df(df_final)
                tabela_html = df_display.to_html(classes="table table-striped", index=False, border=0)
        except Exception as e:
            logger.warning("Falha ao aplicar filtros: %s", e)

        # Se por algum motivo não foi possível montar a tabela filtrada, exibe raw
        if tabela_html is None:
            # Se não houver tabela filtrada, formata o raw para exibição
            df_display = _format_display_df(df)
            tabela_html = df_display.to_html(classes="table table-striped", index=False, border=0)

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
        logger.warning("Erro HTTP ao buscar site: %s (status=%s) [origin=view]", e, status)
        # Logs diagnósticos opcionais e temporários (controle via env var SCRAPE_VERBOSE_LOGGING=1)
        try:
            if os.environ.get('SCRAPE_VERBOSE_LOGGING') == '1':
                resp = getattr(e, 'response', None)
                req_hdrs = None
                resp_hdrs = None
                body_snip = None
                try:
                    if resp is not None:
                        resp_hdrs = dict(resp.headers) if getattr(resp, 'headers', None) is not None else None
                        body_snip = getattr(resp, 'text', '')
                        if isinstance(body_snip, str):
                            body_snip = body_snip[:1000].replace('\n', ' ').replace('\r', ' ')
                        req = getattr(resp, 'request', None)
                        if req is not None:
                            req_hdrs = dict(getattr(req, 'headers', {}))
                except Exception as ex:
                    logger.debug('Erro ao coletar dados de resposta para logging verboso: %s', ex)

                logger.warning('Diagnostic: verbose 403 detected (view). status=%s url=%s', status, url)
                if req_hdrs:
                    logger.warning('Diagnostic: request headers (sample): %s', {k: req_hdrs.get(k) for k in ['User-Agent', 'Accept', 'Referer'] if k in req_hdrs})
                if resp_hdrs:
                    logger.warning('Diagnostic: response headers (sample): %s', {k: resp_hdrs.get(k) for k in ['Server', 'Via', 'X-Cache', 'Content-Type'] if k in resp_hdrs})
                if body_snip:
                    logger.warning('Diagnostic: response body snippet: %s', body_snip)
        except Exception:
            logger.debug('Erro durante logging verboso')
        # Se for 403, grava metadata com status forbidden, last_attempt, next_allowed_attempt e forbidden_count
        try:
            os.makedirs(media_dir, exist_ok=True)

            # Carrega metadata existente (se houver) para contar ocorrências anteriores
            existing = None
            try:
                if os.path.exists(metadata_path):
                    with open(metadata_path, "r", encoding="utf-8") as f:
                        existing = json.load(f)
            except Exception:
                existing = None

            existing_count = int(existing.get("forbidden_count", 0)) if isinstance(existing, dict) else 0
            new_count = existing_count + 1

            # Parâmetros de backoff configuráveis por env vars
            base_hours = int(os.environ.get("SCRAPE_BACKOFF_BASE_HOURS", "2"))
            max_hours = int(os.environ.get("SCRAPE_BACKOFF_MAX_HOURS", "168"))
            # Crescimento exponencial: base * 2^(n-1)
            backoff_hours = min(base_hours * (2 ** (new_count - 1)), max_hours)

            next_allowed = (now() + pd.Timedelta(hours=backoff_hours)).isoformat()
            metadata_forbidden = {
                "last_scrape": meta_current.get("last_scrape") if 'meta_current' in locals() and isinstance(meta_current, dict) else None,
                "last_attempt": now().isoformat(),
                "next_allowed_attempt": next_allowed,
                "status": "forbidden",
                "http_status": status,
                "forbidden_count": new_count,
                "backoff_hours": backoff_hours,
                "source_url": url,
            }

            # Só grava se não houver um `next_allowed_attempt` futuro já presente
            try:
                should_write = True
                if existing and existing.get("status") == "forbidden" and existing.get("next_allowed_attempt"):
                    try:
                        existing_next = datetime.fromisoformat(existing.get("next_allowed_attempt"))
                        if existing_next > now().astimezone(dj_tz.get_default_timezone()):
                            # Se já houver cooldown futuro maior, não sobrescreve, mas incrementa contador local
                            should_write = False
                    except Exception:
                        should_write = True

                if should_write:
                    with open(metadata_path + ".tmp", "w", encoding="utf-8") as f:
                        json.dump(metadata_forbidden, f, ensure_ascii=False, indent=4)
                    os.replace(metadata_path + ".tmp", metadata_path)
                    logger.warning("Gravado metadata forbidden (count=%s backoff=%sh)", new_count, backoff_hours)
                else:
                    # Atualiza apenas contador se necessário (evitar sobrescrever next_allowed)
                    try:
                        existing = existing or {}
                        existing["forbidden_count"] = new_count
                        with open(metadata_path + ".tmp", "w", encoding="utf-8") as f:
                            json.dump(existing, f, ensure_ascii=False, indent=4)
                        os.replace(metadata_path + ".tmp", metadata_path)
                        logger.warning("Cooldown já ativo, atualizado forbidden_count=%s", new_count)
                    except Exception:
                        logger.warning("Falha ao atualizar forbidden_count no metadata")
            except Exception:
                logger.warning("Falha ao verificar/atualizar metadata de forbidden")
        except Exception:
            logger.warning("Não foi possível gravar metadata de forbidden")
        # Fallback: ler cache local
        tabela_html, data_atual = _read_cached_table()
        if tabela_html is not None:
            # Adiciona nota que foi usado cache
            nota = "Dados carregados do cache local."
            if not data_atual:
                data_atual = now().astimezone(dj_tz.get_default_timezone()).strftime("%d/%m/%Y %H:%M")
            nota = f"{nota} Última atualização: {data_atual}."
            tabela_html = tabela_html + f"<p><em>{nota}</em></p>"
            return render(request, "structure/index.html", {"tabela_html": tabela_html, "data_atual": data_atual})
        return render(request, "structure/index.html", {"tabela_html": f"<p>Erro: {e}</p>", "data_atual": None})

    except Exception as e:
        logger.exception("Erro ao buscar/parsear tabela:")
        tabela_html, data_atual = _read_cached_table()
        if tabela_html is not None:
            tabela_html = tabela_html + "<p><em>Dados carregados do cache local.</em></p>"
            return render(request, "structure/index.html", {"tabela_html": tabela_html, "data_atual": data_atual})
        return render(request, "structure/index.html", {"tabela_html": f"<p>Erro: {e}</p>", "data_atual": None})


def _format_display_df(df: pd.DataFrame) -> pd.DataFrame:
    """Formata colunas numéricas para exibição no padrão BR.

    - 'Liq.2meses' -> agrupamento de milhares com '.' sem casas decimais
    - 'Mrg Ebit', 'EV/EBIT', 'P/L' -> duas casas decimais com vírgula
    Mantém valores originais se não conseguirmos converter.
    """
    df2 = df.copy()
    def en_to_br(num, decimals=2, thousands=True):
        try:
            if pd.isna(num):
                return ''
            n = float(num)
        except Exception:
            return str(num)

        if thousands and abs(n) >= 1000:
            fmt = f"{{:,.{decimals}f}}" if decimals > 0 else "{:,.0f}"
            s = fmt.format(n)
            # troca: 1,234.56 -> 1.234,56
            s = s.replace(',', 'X').replace('.', ',').replace('X', '.')
            # se decimals == 0, remover ,00
            if decimals == 0:
                s = s.split(',')[0]
            return s
        else:
            fmt = f"{{:.{decimals}f}}"
            s = fmt.format(n).replace('.', ',')
            return s

    # Liq.2meses como inteiro com separador de milhares
    if 'Liq.2meses' in df2.columns:
        df2['Liq.2meses'] = df2['Liq.2meses'].apply(lambda x: en_to_br(clean_numeric(x), decimals=0, thousands=True) if pd.notna(clean_numeric(x)) else (x if pd.notna(x) else ''))

    for col in ['Mrg Ebit', 'EV/EBIT', 'P/L']:
        if col in df2.columns:
            df2[col] = df2[col].apply(lambda x: en_to_br(clean_numeric(x), decimals=2, thousands=False) if pd.notna(clean_numeric(x)) else (x if pd.notna(x) else ''))

    return df2
