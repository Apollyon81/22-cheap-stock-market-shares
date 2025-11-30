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


class Command(BaseCommand):
    help = 'Scrape the main table from Fundamentus, save raw and filtered'

    def handle(self, *args, **kwargs):
        url = "https://www.fundamentus.com.br/resultado.php"

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
            df_raw.to_csv(raw_path, index=False, encoding='utf-8-sig')
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
            df_final.to_csv(final_path, index=False, encoding='utf-8-sig')
            self.stdout.write(self.style.SUCCESS(f"✔ acoes_filtradas.csv salvo: {final_path}"))

        finally:
            driver.quit()