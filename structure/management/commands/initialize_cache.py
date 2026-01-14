from django.core.management.base import BaseCommand
from django.core.cache import cache
import pandas as pd
import json
import os
from django.conf import settings
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Inicializa o cache Redis com dados locais se estiver vazio'

    def handle(self, *args, **kwargs):
        try:
            self.stdout.write("🔄 Verificando cache Redis...")

            # Verificar se cache já tem dados
            dados_existentes = cache.get('acoes_filtradas')
            metadata_existente = cache.get('metadata')

            if dados_existentes and metadata_existente:
                self.stdout.write(
                    self.style.SUCCESS("✅ Cache Redis já contém dados - pulando inicialização")
                )
                return

            self.stdout.write("📂 Cache vazio - inicializando com dados locais...")

            # Caminhos dos arquivos
            media_dir = os.path.join(settings.BASE_DIR, 'media')
            csv_path = os.path.join(media_dir, 'acoes_filtradas.csv')
            metadata_path = os.path.join(media_dir, 'metadata.json')

            # Carregar dados do CSV local ou S3
            dados_carregados = False
            if os.path.exists(csv_path):
                df = pd.read_csv(csv_path, encoding='utf-8-sig', dtype=str)
                dados_filtrados = df.to_dict('records')
                dados_carregados = True
                self.stdout.write(
                    self.style.SUCCESS(f"✅ Dados carregados do arquivo local: {len(dados_filtrados)} ações")
                )
            else:
                # Tentar carregar do S3 como fallback
                bucket = os.environ.get('AWS_S3_BUCKET')
                if bucket:
                    try:
                        from structure.s3_utils import get_csv_df
                        csv_io = get_csv_df(bucket, 'acoes_filtradas.csv')
                        df = pd.read_csv(csv_io, encoding='utf-8-sig', dtype=str)
                        dados_filtrados = df.to_dict('records')
                        dados_carregados = True
                        self.stdout.write(
                            self.style.SUCCESS(f"✅ Dados carregados do S3: {len(dados_filtrados)} ações")
                        )
                    except Exception as e:
                        self.stdout.write(
                            self.style.WARNING(f"⚠️ Falha ao carregar do S3: {e}")
                        )

            if dados_carregados:
                # Salvar no cache Redis
                cache.set('acoes_filtradas', dados_filtrados, timeout=None)
            else:
                self.stdout.write(
                    self.style.WARNING("⚠️ Nenhum arquivo CSV encontrado (local ou S3)")
                )

            # Carregar metadata local ou S3
            metadata_carregada = False
            if os.path.exists(metadata_path):
                with open(metadata_path, 'r', encoding='utf-8') as f:
                    metadata = json.load(f)
                metadata_carregada = True
                self.stdout.write(
                    self.style.SUCCESS(f"✅ Metadata carregada do arquivo local")
                )
            else:
                # Tentar carregar do S3 como fallback
                bucket = os.environ.get('AWS_S3_BUCKET')
                if bucket:
                    try:
                        from structure.s3_utils import get_json
                        metadata = get_json(bucket, 'metadata.json')
                        metadata_carregada = True
                        self.stdout.write(
                            self.style.SUCCESS(f"✅ Metadata carregada do S3")
                        )
                    except Exception as e:
                        self.stdout.write(
                            self.style.WARNING(f"⚠️ Falha ao carregar metadata do S3: {e}")
                        )

            if metadata_carregada:
                # Salvar no cache Redis
                cache.set('metadata', metadata, timeout=None)

                status = metadata.get('status', 'unknown')
                rows = metadata.get('rows_filtered', 0)
                last_scrape = metadata.get('last_scrape', 'unknown')

                self.stdout.write(
                    self.style.SUCCESS(f"✅ Metadata processada: status={status}, ações={rows}")
                )
                self.stdout.write(f"📅 Última atualização: {last_scrape}")
            else:
                self.stdout.write(
                    self.style.WARNING("⚠️ Arquivo metadata.json não encontrado (local ou S3)")
                )

            # Verificação final
            dados_verificacao = cache.get('acoes_filtradas')
            metadata_verificacao = cache.get('metadata')

            if dados_verificacao and metadata_verificacao:
                self.stdout.write(
                    self.style.SUCCESS("🎉 Inicialização de cache concluída com sucesso!")
                )
            else:
                self.stdout.write(
                    self.style.WARNING("⚠️ Cache pode não ter sido populado corretamente")
                )

        except Exception as e:
            self.stdout.write(
                self.style.ERROR(f"❌ Erro durante inicialização do cache: {e}")
            )
            logger.exception("Erro no comando initialize_cache")
            raise
