# syntax=docker/dockerfile:1
FROM python:3.10.5-slim-buster
WORKDIR /home/debian/docker/wikipedia-crawler
# Add begin
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt
# Add end
# COPY . /app/
CMD ["python", "wikipedia-crawler.py"]
