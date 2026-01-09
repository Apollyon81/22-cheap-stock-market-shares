from django.shortcuts import render
from urllib.request import Request, urlopen
from urllib.error import HTTPError, URLError
from bs4 import BeautifulSoup
import pandas as pd

def home(request):
    try:
        # URL do site de origem
        url = "https://www.fundamentus.com.br/resultado.php"
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
        }
        req = Request(url, headers=headers)
        with urlopen(req) as resp:
            html = resp.read().decode('utf-8', errors='replace')

        # Parsear o HTML com BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # Extrair os dados da tabela (exemplo simplificado)
        table = soup.find("table")
        if table is None:
            raise ValueError("Tabela não encontrada na página")
        df = pd.read_html(str(table))[0]

        # Filtrar e formatar os dados (exemplo simplificado)
        df = df[df["Liquidez"] > 1000000]  # Exemplo de filtro
        tabela_html = df.to_html(classes="table table-striped", index=False, border=0)

        return render(request, "structure/index.html", {"tabela_html": tabela_html})
    except Exception as e:
        return render(request, "structure/index.html", {"tabela_html": f"<p>Erro: {e}</p>"})