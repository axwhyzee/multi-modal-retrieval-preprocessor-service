FROM python:3.11-slim

COPY . .        

RUN apt-get update && apt-get install -y git ffmpeg

RUN pip install -r requirements.txt

EXPOSE 5000