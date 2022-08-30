export DATALAKE_FOLDER="./raw"

docker compose rm
docker compose build
docker compose up -d