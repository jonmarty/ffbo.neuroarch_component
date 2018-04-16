DOCKER_NAME = "ffbo.neuroarch_component"
if [ "$#" -eq 1 ]
then
    DOCKER_NAME = $1
fi

docker rm $DOCKER_NAME
docker run --name $DOCKER_NAME -v $(dirname `pwd`):/neuroarch_component -v $(dirname $(dirname `pwd`))/neuroarch:/neuroarch -it ffbo/neuroarch_component:develop sh /neuroarch_component/neuroarch_component/run_component_docker.sh
