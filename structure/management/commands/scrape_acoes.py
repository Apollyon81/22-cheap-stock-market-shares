from django.core.management.base import BaseCommand
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from io import StringIO
import time

class Command(BaseCommand):
    help = 'Scrape the main table from Fundamentus, clean numeric columns, and print'

    def handle(self, *args, **kwargs):
        url = "https://www.fundamentus.com.br/resultado.php"

        # Configuração do Chrome em modo headless
        options = Options()
        options.add_argument("--headless=new")  
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        # Inicia o driver
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        try:
            driver.get(url)
            time.sleep(3) 

            # Pega a tabela pelo ID
            table_el = driver.find_element(By.ID, "resultado")
            table_html = table_el.get_attribute("outerHTML")

            # Lê a tabela com pandas
            df = pd.read_html(StringIO(table_html))[0]

            # Limpeza de colunas numéricas
            for col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace('.', '', regex=False)   # remove milhar
                    .str.replace(',', '.', regex=False)  # vírgula -> ponto
                    .str.replace('%', '', regex=False)   # remove %
                    .str.replace('-', '', regex=False)   # remove traço
                )
                df[col] = pd.to_numeric(df[col], errors='ignore')
            
            # =========================
            # Aplicar filtros
            # =========================
            df = df[df['Liq.2meses'] >= 1000000]  # Liquidez >= 1 milhão
            df = df[df['Mrg Ebit'] > 0]           # EBIT margin positiva
            df = df[df['EV/EBIT'] > 0]            # EV/EBIT positivo
            df = df[df['P/L'] > 0]                # Lucro por ação positivo (EPS)

            # Ordenar por EV/EBIT ascendente e Mrg Ebit descendente
            df = df.sort_values(by=['EV/EBIT', 'Mrg Ebit'], ascending=[True, False])

            # Selecionar top 22 ações
            df_top22 = df.head(22)

            # =========================
            # Imprimir resultados filtrados
            # =========================
            self.stdout.write("\n[FILTRO] Top 22 ações após aplicar filtros:")
            self.stdout.write(df_top22.to_string(index=False))

            # Mostrar nomes das colunas
            self.stdout.write("\n[INFO] Colunas disponíveis:")
            self.stdout.write(str(list(df_top22.columns)))

            # =========================
            # Exportar para CSV
            # =========================
            output_path = 'media/acoes_filtradas.csv'  # pasta media/ deve existir
            df_top22.to_csv(output_path, index=False, encoding='utf-8-sig')
            self.stdout.write(f"\n[TABELA SALVA] Arquivo CSV gerado em: {output_path}")


        finally:
            driver.quit()


