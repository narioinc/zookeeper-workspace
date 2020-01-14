#stop kafka containers
docker ps -a | awk '{ print $1,$2 }' | grep spotify/kafka | awk '{print $1 }' | xargs -I {} docker stop {}
docker ps -a | awk '{ print $1,$2 }' | grep centos:7 | awk '{print $1 }' | xargs -I {} docker rm {}
