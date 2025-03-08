# Preprocessor Service

## Setup
Setup Python environment
```
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

Install additional dependencies
```
# for manipulating PDFs
brew install poppler-utils

# for splitting videos
brew install ffmpeg  
```

Run tests
```
# pull test files from git lfs
git lfs install
git lfs pull
pytest
```

Ensure you have a `.env` file with the following env variables before starting the application
```
REDIS_HOST=...
REDIS_PORT=...
REDIS_USERNAME=...
REDIS_PASSWORD=...

STORAGE_SERVICE_API_URL=http://host.docker.internal:5001/
```

## Run
```
python app.py
```
