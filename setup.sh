cd "docker"

docker-compose up --build -d


while ! docker-compose ps | grep -q "Up"; do
    sleep 1
done  