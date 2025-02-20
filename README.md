# Preprocessor Service

## Setup python env
```
python3 -m venv env
source env/bin/activate
pip install -r requirements.txt
```

## Install additional dependencies
Skip the below steps if you intend to just run the docker container. The following libraries are included in the dockerfile already.
```
# for generating thumbnails from text
brew install exiftool
brew install imagemagick  

# for splitting videos
brew install ffmpeg  
```

## Run tests
```
# pull test files from git lfs
git lfs install
git lfs pull
pytest
```

## Run docker container
First, ensure you have a `.env` file with the following env variables
```
REDIS_HOST=...
REDIS_PORT=...
REDIS_USERNAME=...
REDIS_PASSWORD=...

STORAGE_SERVICE_API_URL=http://host.docker.internal:5001/
```

Next, build the docker image and run container
```
docker-compose up
```