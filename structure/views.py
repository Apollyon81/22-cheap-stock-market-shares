from django.shortcuts import render
import pandas as pd
import os
from django.conf import settings
import logging
from django.utils.timezone import now
from datetime import datetime
import json
from django.utils import timezone as dj_tz

logger = logging.getLogger(__name__)

def home(request):
           try:
               # Tentar ler do Redis primeiro (dados compartilhados entre containers)
               from django.core.cache import cache

               dados_filtrados = None
               metadata_cache = None

               try:
                   dados_filtrados = cache.get('acoes_filtradas')
                   metadata_cache = cache.get('metadata')
               except:
                   # Redis não disponível (desenvolvimento local)
                   pass

               if dados_filtrados and metadata_cache:
                   # Dados disponíveis no Redis (produção)
                   df = pd.DataFrame(dados_filtrados)
                   dados_desatualizados = False
               else:
                   # Fallback: ler do CSV local (desenvolvimento/local)
                   csv_path = os.path.join(settings.BASE_DIR, 'media', 'acoes_filtradas.csv')

                   if not os.path.exists(csv_path):
                       mensagem = (
                           "<p style='padding: 20px; text-align: center;'>"
                           "<strong>Dados não disponíveis.</strong><br>"
                           "Aguarde a próxima atualização automática ou execute <code>python manage.py scrape_data</code>."
                           "</p>"
                       )
                       return render(request, "structure/index.html", {"tabela_html": mensagem})

                   # Lê o CSV local
                   df = pd.read_csv(
                       csv_path,
                       encoding='utf-8-sig',
                       dtype=str
                   )
                   dados_desatualizados = True  # Dados locais podem estar desatualizados
        
        # Verifica se os dados estão desatualizados (para possível atualização automática)
        metadata_path = os.path.join(settings.BASE_DIR, 'media', 'metadata.json')
        dados_desatualizados = False
        data_atual = ''

        if os.path.exists(metadata_path):
            try:
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    meta = json.load(f)
                last_scrape = meta.get("last_scrape")
                if last_scrape:
                    # converte para timezone local e formata como dd/mm/YYYY
                    try:
                        last_dt = datetime.fromisoformat(last_scrape)
                        # converte para timezone do Django
                        if last_dt.tzinfo is None:
                            last_dt = dj_tz.make_aware(last_dt, dj_tz.UTC)
                        last_local = last_dt.astimezone(dj_tz.get_current_timezone())
                        data_atual = ' - ' + last_local.strftime('%d/%m/%Y')
                        hoje = now().astimezone(dj_tz.get_current_timezone()).date()
                        dados_desatualizados = (last_local.date() < hoje)
                    except Exception:
                        # falha ao parsear -> marca como desatualizado para acionar scraping
                        dados_desatualizados = True
            except Exception:
                # Metadata não pôde ser lido
                dados_desatualizados = True
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

        return render(request, "structure/index.html", {"tabela_html": tabela_html, "data_atual": data_atual})

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
