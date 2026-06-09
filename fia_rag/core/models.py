import uuid
from django.db import models
from django.contrib.auth.models import AbstractUser

class User(AbstractUser):
    daily_quota = models.IntegerField(default=20)
    queries_today = models.IntegerField(default=0)
    quota_reset_date = models.DateField(auto_now_add=True)

class ChatSession(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)

class Message(models.Model):
    session = models.ForeignKey(ChatSession, on_delete=models.CASCADE, related_name='messages')
    role = models.CharField(max_length=10)  # 'user' or 'assistant'
    content = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

class IngestionQueue(models.Model):
    """Tracks newly extracted .md files waiting to enter DuckDB."""
    STATUS_CHOICES = [
        ('pending', 'Pending'), ('processing', 'Processing'),
        ('done', 'Done'), ('failed', 'Failed')
    ]
    manifest_key = models.CharField(max_length=512, unique=True)  # e.g. 'documents/2024/.../doc.pdf'
    source_hash = models.CharField(max_length=80)                  # sha256 from manifest.json
    status = models.CharField(max_length=12, choices=STATUS_CHOICES, default='pending')
    error_message = models.TextField(blank=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
