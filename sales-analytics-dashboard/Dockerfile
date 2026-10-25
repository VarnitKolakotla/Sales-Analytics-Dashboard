FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
ENV PORT=5000
EXPOSE 5000
# one worker: DuckDB already uses all CPU cores for every query; scale out with more containers
CMD ["gunicorn", "-w", "1", "--threads", "8", "-t", "120", "-b", "0.0.0.0:5000", "app:app"]
