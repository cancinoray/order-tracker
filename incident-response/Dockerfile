FROM python:3.12-slim
WORKDIR /srv
COPY main.py ./
RUN pip install --no-cache-dir "fastapi>=0.115,<1" "uvicorn>=0.30,<1" "httpx>=0.27,<1"
ENV RESPONDER_DATA_DIR=/data
EXPOSE 8001
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001"]
