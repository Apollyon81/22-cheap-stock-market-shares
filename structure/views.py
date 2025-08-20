from django.shortcuts import render
import pandas as pd
import os
from django.conf import settings

def home(request):
    df = pd.read_csv(os.path.join(settings.BASE_DIR, 'media', 'acoes_filtradas.csv'), encoding='utf-8-sig')

    df = df[df['Liq.2meses'] >= 1000000]
    df = df[df['Mrg Ebit'] > 0]
    df = df[df['EV/EBIT'] > 0]
    df = df[df['P/L'] > 0]
    df = df.drop_duplicates(subset='Papel', keep='first')
    df = df.sort_values(by=['EV/EBIT', 'Mrg Ebit'], ascending=[True, False])
    df_top22 = df.head(22)

    # Seleciona apenas as colunas que serão mostradas
    colunas_para_exibir = ['Papel', 'Liq.2meses', 'Mrg Ebit', 'EV/EBIT', 'P/L']
    df_top22 = df_top22[colunas_para_exibir]


    tabela_html = df_top22.to_html(classes="table table-striped", index=False, border=0)

    return render(request, "structure/index.html", {"tabela_html": tabela_html})



