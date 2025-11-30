# structure/apps.py
from django.apps import AppConfig
import threading
from django.core.management import call_command
import os

class StructureConfig(AppConfig):
    default_auto_field = "django.db.models.BigAutoField"
    name = "structure"

    def ready(self):
        # Função para criar CSV se não existir
        def ensure_csv():
            os.makedirs('media', exist_ok=True)
            if not os.path.exists('media/acoes_filtradas.csv'):
                print("CSV não encontrado, gerando...")
                try:
                    call_command("scrape_data")  # ou scrape_acoes se não mudou o nome
                    print("CSV criado com sucesso!")
                except Exception as e:
                    print(f"Erro ao gerar CSV: {e}")

        threading.Thread(target=ensure_csv).start()
