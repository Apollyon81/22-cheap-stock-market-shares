from django.core.management.base import BaseCommand
from django.core.cache import cache
import os
import logging

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Verifica o status dos workers Celery e conectividade dos serviços'

    def handle(self, *args, **kwargs):
        self.stdout.write("🔍 Verificando status do sistema...\n")

        # 1. Verificar Redis
        self.stdout.write("1. 🔗 Testando conexão Redis...")
        try:
            cache.set('test_key', 'test_value', timeout=10)
            test_value = cache.get('test_key')
            if test_value == 'test_value':
                self.stdout.write(self.style.SUCCESS("   ✅ Redis conectado e funcional"))
            else:
                self.stdout.write(self.style.ERROR("   ❌ Redis retornou valor incorreto"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"   ❌ Erro na conexão Redis: {e}"))

        # 2. Verificar dados em cache
        self.stdout.write("\n2. 📊 Verificando dados em cache...")
        try:
            dados_cache = cache.get('acoes_filtradas')
            metadata_cache = cache.get('metadata')

            if dados_cache:
                self.stdout.write(self.style.SUCCESS(f"   ✅ Dados em cache: {len(dados_cache)} ações"))
            else:
                self.stdout.write(self.style.WARNING("   ⚠️ Nenhum dado encontrado em cache"))

            if metadata_cache:
                status = metadata_cache.get('status', 'unknown')
                rows = metadata_cache.get('rows_filtered', 0)
                last_scrape = metadata_cache.get('last_scrape', 'unknown')
                self.stdout.write(self.style.SUCCESS(f"   ✅ Metadata em cache: status={status}, ações={rows}"))
                self.stdout.write(f"   📅 Última atualização: {last_scrape}")
            else:
                self.stdout.write(self.style.WARNING("   ⚠️ Metadata não encontrada em cache"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"   ❌ Erro ao acessar cache: {e}"))

        # 3. Verificar arquivos locais
        self.stdout.write("\n3. 📁 Verificando arquivos locais...")
        from django.conf import settings
        media_dir = os.path.join(settings.BASE_DIR, 'media')

        arquivos_verificar = [
            'acoes_filtradas.csv',
            'acoes_raw.csv',
            'metadata.json'
        ]

        for arquivo in arquivos_verificar:
            caminho = os.path.join(media_dir, arquivo)
            if os.path.exists(caminho):
                tamanho = os.path.getsize(caminho)
                self.stdout.write(self.style.SUCCESS(f"   ✅ {arquivo}: {tamanho} bytes"))
            else:
                self.stdout.write(self.style.WARNING(f"   ⚠️ {arquivo}: não encontrado"))

        # 4. Verificar configuração S3
        self.stdout.write("\n4. ☁️ Verificando configuração S3...")
        bucket = os.environ.get('AWS_S3_BUCKET')
        access_key = os.environ.get('AWS_ACCESS_KEY_ID')
        secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

        if bucket and access_key and secret_key:
            self.stdout.write(self.style.SUCCESS(f"   ✅ S3 configurado: bucket={bucket}"))
            # Testar conexão S3
            try:
                from structure.s3_utils import _get_s3_client
                client = _get_s3_client()
                # Tentar listar objetos (teste básico de conectividade)
                response = client.list_objects_v2(Bucket=bucket, MaxKeys=1)
                self.stdout.write(self.style.SUCCESS("   ✅ Conexão S3 estabelecida"))
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"   ❌ Erro na conexão S3: {e}"))
        else:
            self.stdout.write(self.style.WARNING("   ⚠️ S3 não configurado (variáveis de ambiente ausentes)"))

        # 5. Verificar Celery
        self.stdout.write("\n5. ⚙️ Verificando configuração Celery...")
        try:
            # Tentar importar e verificar configuração
            from django.conf import settings
            if hasattr(settings, 'CELERY_BROKER_URL') or hasattr(settings, 'CELERY_RESULT_BACKEND'):
                self.stdout.write(self.style.SUCCESS("   ✅ Celery configurado"))
            else:
                self.stdout.write(self.style.WARNING("   ⚠️ Celery pode não estar totalmente configurado"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"   ❌ Erro na configuração Celery: {e}"))

        # 6. Status geral
        self.stdout.write("\n" + "="*50)
        self.stdout.write("📋 STATUS GERAL DO SISTEMA")
        self.stdout.write("="*50)

        # Verificar se sistema está operacional
        redis_ok = cache.get('test_key') == 'test_value'
        dados_ok = cache.get('acoes_filtradas') is not None
        metadata_ok = cache.get('metadata') is not None

        if redis_ok and dados_ok and metadata_ok:
            self.stdout.write(self.style.SUCCESS("🎉 SISTEMA TOTALMENTE OPERACIONAL"))
            self.stdout.write("✅ Redis: OK")
            self.stdout.write("✅ Dados: OK")
            self.stdout.write("✅ Metadata: OK")
        elif redis_ok and (dados_ok or metadata_ok):
            self.stdout.write(self.style.WARNING("⚠️ SISTEMA PARCIALMENTE OPERACIONAL"))
            self.stdout.write("✅ Redis: OK")
            self.stdout.write(f"{'✅' if dados_ok else '❌'} Dados: {'OK' if dados_ok else 'FALHA'}")
            self.stdout.write(f"{'✅' if metadata_ok else '❌'} Metadata: {'OK' if metadata_ok else 'FALHA'}")
        else:
            self.stdout.write(self.style.ERROR("❌ SISTEMA COM PROBLEMAS"))
            self.stdout.write(f"{'✅' if redis_ok else '❌'} Redis: {'OK' if redis_ok else 'FALHA'}")
            self.stdout.write(f"{'✅' if dados_ok else '❌'} Dados: {'OK' if dados_ok else 'FALHA'}")
            self.stdout.write(f"{'✅' if metadata_ok else '❌'} Metadata: {'OK' if metadata_ok else 'FALHA'}")

        # Limpar chave de teste
        try:
            cache.delete('test_key')
        except:
            pass
