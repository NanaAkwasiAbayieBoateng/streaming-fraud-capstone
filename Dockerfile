FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt requirements-kafka.txt ./
RUN pip install --no-cache-dir -r requirements.txt -r requirements-kafka.txt

COPY streaming_fraud/ streaming_fraud/
COPY scripts/ scripts/
COPY conftest.py ./

CMD ["python", "scripts/run_local_demo.py"]
