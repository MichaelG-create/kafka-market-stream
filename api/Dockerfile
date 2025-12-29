FROM python:3.12-slim
WORKDIR /app
RUN pip install flask duckdb flask-cors pandas  # ← ADD pandas
COPY duckdb-api.py .
CMD ["python", "duckdb-api.py"]
EXPOSE 8080
