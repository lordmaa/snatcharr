FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ARG BUILD_SHA=dev
ENV BUILD_SHA=${BUILD_SHA}
ENV SNATCHARR_DATA=/data
ENV SNATCHARR_PORT=6060
VOLUME ["/data"]

EXPOSE 6060

CMD ["python3", "app.py"]
