# invest22/__init__.py
# Importação opcional do Celery para evitar erro durante deploy se não estiver instalado
try:
    from .celery import app as celery_app
    __all__ = ['celery_app']
except (ImportError, ModuleNotFoundError):
    # Celery não está disponível (pode acontecer durante deploy ou se não estiver instalado)
    celery_app = None
    __all__ = []
