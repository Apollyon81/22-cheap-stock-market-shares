from django.shortcuts import render
import pandas as pd
import os
from django.conf import settings
import logging
from django.utils.timezone import now
from datetime import datetime
import json

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
        
        # Verifica se os dados estão desatualizados (para possível atualização automática)
        metadata_path = os.path.join(settings.BASE_DIR, 'media', 'metadata.json')
        dados_desatualizados = False
        
        if os.path.exists(metadata_path):
            with open(metadata_path, 'r', encoding='utf-8') as f:
                meta = json.load(f)
            last_scrape = meta.get("last_scrape")
            
            if last_scrape:
                last_dt = datetime.fromisoformat(last_scrape)
                hoje = now().date()
                # Detecta se scraping não é de hoje
                dados_desatualizados = (last_dt.date() < hoje)
        else:
            # Metadata não existe = nunca foi feito scraping
            dados_desatualizados = True
        
        # dados_desatualizados pode ser usado para acionar atualização automática se necessário


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
