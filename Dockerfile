FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py pulse_template.html ./

RUN mkdir -p /app/www /app/cache

EXPOSE 8080

# без -u вивід print() буферизується і не потрапляє в docker/kubectl logs,
# поки буфер не заповниться (а він може й не заповнитись за годинами)
CMD ["python", "-u", "app.py"]
