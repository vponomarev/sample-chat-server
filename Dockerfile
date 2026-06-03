FROM python:3.14-slim

WORKDIR /app

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода
COPY server/ ./server/
COPY client/ ./client/

# Создание директории для БД
RUN mkdir -p /app/data

# Переменные окружения
ENV PYTHONUNBUFFERED=1
ENV HOST=0.0.0.0
ENV DB_PATH=/app/data/chat.db

# Порт по умолчанию
EXPOSE 8080

# Запуск сервера
CMD ["python", "server/main.py"]
