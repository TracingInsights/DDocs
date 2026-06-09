"""
WSGI config for fia_project.
"""
import os
from django.core.wsgi import get_wsgi_application

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'fia_project.settings')

application = get_wsgi_application()
