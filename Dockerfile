FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py pulse_template.html ./

RUN mkdir -p /app/www /app/cache

EXPOSE 8080

CMD ["python", "app.py"]
