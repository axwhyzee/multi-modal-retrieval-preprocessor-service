## Install additional dependencies
```
brew install imagemagick  # for generating thumbnails from text
brew install ffmpeg  # for splitting videos
```

## Run tests
```
# pull test files from git lfs
git lfs install
git lfs pull
pytest
```