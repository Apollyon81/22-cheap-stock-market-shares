from django.shortcuts import render
import pandas as pd
import os
from django.conf import settings
from .filters import apply_filters, format_output
import logging

logger = logging.getLogger(__name__)

def home(request):
    try:
        # Lê o arquivo CSV
        df = pd.read_csv(os.path.join(settings.BASE_DIR, 'media', 'acoes_filtradas.csv'), encoding='utf-8-sig')
        
        # Aplica os filtros
        df_filtered = apply_filters(df)
        
        # Formata os valores para exibição
        df_formatted = format_output(df_filtered)
        
        # Converte para HTML
        tabela_html = df_formatted.to_html(classes="table table-striped", index=False, border=0)
        
        return render(request, "structure/index.html", {"tabela_html": tabela_html})
        
    except Exception as e:
        logger.error(f"Erro ao processar dados: {str(e)}")
        return render(request, "structure/index.html", {"erro": "Erro ao processar os dados."})


    tabela_html = df_top22.to_html(classes="table table-striped", index=False, border=0)

    return render(request, "structure/index.html", {"tabela_html": tabela_html})



