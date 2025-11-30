from django.shortcuts import render
import pandas as pd
import os
from django.conf import settings
import logging

logger = logging.getLogger(__name__)

def home(request):
    try:
        # Caminho do arquivo CSV
        csv_path = os.path.join(settings.BASE_DIR, 'media', 'acoes_filtradas.csv')
        
        # Verifica se o arquivo existe
        if not os.path.exists(csv_path):
            mensagem = (
                "<p style='padding: 20px; text-align: center;'>"
                "<strong>Arquivo CSV não encontrado.</strong><br>"
                "Execute o comando <code>python manage.py scrape_data</code> para gerar os dados."
                "</p>"
            )
            return render(request, "structure/index.html", {"tabela_html": mensagem})
        
        # Lê o CSV já filtrado pelo comando scrape_data
        # Os dados já vêm formatados corretamente do filters.py
        df = pd.read_csv(
            csv_path,
            encoding='utf-8-sig',
            dtype=str  # mantém formato brasileiro
        )
        
        # Verifica se o DataFrame está vazio
        if df.empty:
            mensagem = (
                "<p style='padding: 20px; text-align: center;'>"
                "<strong>Nenhum dado disponível.</strong><br>"
                "O arquivo CSV está vazio. Execute o comando <code>python manage.py scrape_data</code> novamente."
                "</p>"
            )
            return render(request, "structure/index.html", {"tabela_html": mensagem})

        # Converte diretamente para HTML (formatação já feita em filters.py)
        tabela_html = df.to_html(
            classes="table table-striped",
            index=False,
            border=0
        )

        return render(request, "structure/index.html", {"tabela_html": tabela_html})

    except FileNotFoundError:
        mensagem = (
            "<p style='padding: 20px; text-align: center;'>"
            "<strong>Arquivo CSV não encontrado.</strong><br>"
            "Execute o comando <code>python manage.py scrape_data</code> para gerar os dados."
            "</p>"
        )
        return render(request, "structure/index.html", {"tabela_html": mensagem})
    except Exception as e:
        logger.exception("Erro ao carregar página principal:")
        mensagem = (
            f"<p style='padding: 20px; text-align: center; color: red;'>"
            f"<strong>Erro ao processar dados:</strong><br>{str(e)}"
            f"</p>"
        )
        return render(request, "structure/index.html", {"tabela_html": mensagem})
