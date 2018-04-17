DOCKER_NAME="ffbo.neuroarch_component"
DATABASE_LOCATION="$(dirname `pwd`)/../ffbo_db"
if [ "$#" -ge 1 ]
then
    DATABASE_LOCATION=$1
fi

if [ "$#" -ge 2 ]
then
    DOCKER_NAME=$2
fi

docker rm $DOCKER_NAME
docker run --name $DOCKER_NAME -v $DATABASE_LOCATION:/opt/orientdb/databases/na_server -v $(dirname `pwd`):/neuroarch_component -v $(dirname $(dirname `pwd`))/neuroarch:/neuroarch -it ffbo/neuroarch_component:develop sh /neuroarch_component/neuroarch_component/run_component_docker.sh
