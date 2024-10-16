FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt /app/

COPY app/ /app/

RUN pip install --no-cache-dir -r requirements.txt

RUN mkdir -p lake/1_bronze

CMD ["python", "extract_brewery_data.py"]
