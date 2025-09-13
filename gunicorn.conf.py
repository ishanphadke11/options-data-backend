import os

bind = f"0.0.0.0:{os.environ.get('PORT', 5000)}"
workers = 2
timeout = 120
keepalive = 2
max_requests = 1000
max_requests_jitter = 100