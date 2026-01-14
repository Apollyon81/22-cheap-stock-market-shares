# structure/apps.py
from django.apps import AppConfig
import threading
from django.core.management import call_command
import os

class StructureConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "structure"

    def ready(self):
        # Inicializar cache Redis com dados locais se vazio
        def initialize_cache():
            try:
                from django.core.cache import cache
                import pandas as pd
                import json

                # Verificar se cache Redis está vazio
                if not cache.get('acoes_filtradas') or not cache.get('metadata'):
                    print("Cache Redis vazio - inicializando com dados locais...")

                    media_dir = 'media'
                    csv_path = os.path.join(media_dir, 'acoes_filtradas.csv')
                    metadata_path = os.path.join(media_dir, 'metadata.json')

                    # Carregar dados do CSV local
                    if os.path.exists(csv_path):
                        df = pd.read_csv(csv_path, encoding='utf-8-sig', dtype=str)
                        dados_filtrados = df.to_dict('records')
                        cache.set('acoes_filtradas', dados_filtrados, timeout=None)
                        print(f"✅ Cache Redis populado com {len(dados_filtrados)} ações")

                    # Carregar metadata
                    if os.path.exists(metadata_path):
                        with open(metadata_path, 'r', encoding='utf-8') as f:
                            metadata = json.load(f)
                            cache.set('metadata', metadata, timeout=None)
                            print(f"✅ Metadata cacheada: {metadata.get('status', 'unknown')}")

                    print("✅ Inicialização de cache concluída")

            except Exception as e:
                print(f"❌ Erro ao inicializar cache: {e}")

        # Executar inicialização em thread separada
        threading.Thread(target=initialize_cache).start()

        # Função para criar CSV se não existir (fallback)
        def ensure_csv():
            os.makedirs('media', exist_ok=True)
            if not os.path.exists('media/acoes_filtradas.csv'):
                print("CSV não encontrado, gerando...")
                try:
                    call_command("scrape_data")
                    print("CSV criado com sucesso!")
                except Exception as e:
                    print(f"Erro ao gerar CSV: {e}")

        threading.Thread(target=ensure_csv).start()
