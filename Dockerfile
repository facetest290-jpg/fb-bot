FROM mcr.microsoft.com/playwright/python:v1.48.0-jammy
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir playwright==1.48.0 tzdata
COPY fb_watcher.py .
CMD ["python", "fb_watcher.py", "--cron"]
