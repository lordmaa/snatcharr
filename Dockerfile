FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV SNATCHARR_DATA=/data
VOLUME ["/data"]

EXPOSE 6060

CMD ["python3", "app.py"]
