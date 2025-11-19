FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y build-essential libpq-dev gcc && rm -rf /var/lib/apt/lists/*

COPY app.py .
COPY requirements.txt .
COPY Procfile .
COPY templates ./templates

RUN pip install --upgrade pip && pip install -r requirements.txt

RUN mkdir -p uploads

EXPOSE 5000

CMD ["python", "app.py"]
