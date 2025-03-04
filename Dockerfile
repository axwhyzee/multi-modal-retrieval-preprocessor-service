FROM python:3.11-slim

COPY . .        

RUN apt-get update && apt-get install -y \
    git \
    ffmpeg \
    poppler-utils \
    build-essential \
    make \
    gcc \
    libmupdf-dev \
    mupdf-tools \
    && rm -rf /var/lib/apt/lists/*

RUN pip install -r requirements.txt

EXPOSE 5000