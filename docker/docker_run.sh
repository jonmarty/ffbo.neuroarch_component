docker rm neuroarch_component
docker run --name neuroarch_component -v $(dirname `pwd`):/neuroarch_component -v $(dirname $(dirname `pwd`))/neuroarch:/neuroarch -it ffbo/neuroarch_component:develop sh /neuroarch_component/neuroarch_component/run_component_docker.sh
