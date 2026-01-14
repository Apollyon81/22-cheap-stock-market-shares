# invest22/celery.py
# Importação opcional do Celery para evitar erro durante deploy se não estiver instalado
try:
    from celery import Celery
except (ImportError, ModuleNotFoundError):
    # Celery não está disponível - criar objetos dummy para evitar erros
    class DummyCelery:
        def __init__(self, *args, **kwargs):
            pass
        def config_from_object(self, *args, **kwargs):
            pass
        def autodiscover_tasks(self):
            pass
        def task(self, *args, **kwargs):
            def decorator(func):
                return func
            return decorator
    Celery = DummyCelery

# Configuração comum (funciona tanto com Celery real quanto dummy)
import os
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'invest22.settings')

app = Celery('invest22')

# Usar string aqui significa que o worker não precisa serializar
# a configuração do objeto para processos filhos.
app.config_from_object('django.conf:settings', namespace='CELERY')

# Auto-descoberta de tarefas dos apps Django
app.autodiscover_tasks()

@app.task(bind=True)
def debug_task(self):
    print(f'Request: {self.request!r}')
