#!/bin/bash

# Check if the wikipedia-crawler Docker container is running
if [ $(docker ps -q -f name=wikipedia-crawler) ]; then
    echo "wikipedia-crawler Docker container is already running."
else
    # Start the wikipedia-crawler container if it is not running
    cd /home/debian/docker/wikipedia-crawler
    docker build -t wikipedia-crawler-python-app .
    # docker run -it --rm --network="host" -v $(pwd):/home/debian/docker/wikipedia-crawler --name wikipedia-crawler wikipedia-crawler-python-app
    docker run -d --rm --network="host" -v $(pwd):/home/debian/docker/wikipedia-crawler --name wikipedia-crawler wikipedia-crawler-python-app
    echo "wikipedia-crawler Docker container started."
fi
