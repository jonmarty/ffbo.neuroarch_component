# FFBO NeuroArch Database Component
[![Twitter Follow](https://img.shields.io/twitter/follow/flybrainobs.svg?style=social&label=Follow)](https://twitter.com/flybrainobs) ![license](https://img.shields.io/github/license/fruitflybrain/ffbo.neuroarch_component.svg?style=flat-square) ![GitHub last commit](https://img.shields.io/github/last-commit/fruitflybrain/ffbo.neuroarch_component.svg?style=flat-square) [![Docker Build Status](https://img.shields.io/docker/build/fruitflybrain/ffbo.neuroarch_component.svg?style=flat-square)](https://hub.docker.com/r/fruitflybrain/ffbo.neuroarch_component)
## Overview

This package contains the NeuroArch component in the backend of the system architecture of the [Fruit Fly Brain Observatory](http://fruitflybrain.org/) (FFBO). It hosts a [NeuroArch Database](http://dx.doi.org/10.5281/zenodo.44225) where fly brain data are stored.

## Installation and Execution

Options for installing and running ffbo.neuroarch_component are explained below.

__NOTE__ If you are using a Docker image to run ffbo.neuroarch_component, you will need the 'ffbonet' network initialized. You can check to see if it exists via

    docker network ls

If it does not, it can be initialized via

    docker network create -d bridge ffbonet

Please note that the 'bridge' driver provides a network that is limited to the host machine's Docker daemon, so images within it cannot communicate with external docker hosts or Docker daemons. If you would like to communicate between computers, please use the 'overlay' driver

    docker network create -d overlay ffbonet

### Docker Hub

Installing via the [Docker Hub](https://hub.docker.com/r/fruitflybrain/ffbo.neuroarch_component) repository is recommended for non-developers. The image is installed directly onto your local Docker daemon, from which you can run it in a container. Installation is as follows:

    docker pull fruitflybrain/ffbo.neuroarch_component

Once the image is installed, you can run it in a container:

    docker run -P -t --net ffbonet --name ffbo.neuroarch_component fruitflybrain/ffbo.neuroarch_component


### Github with Docker Compose

Installing via the [Github](https://github.com/fruitflybrain/ffbo.neuroarch_component) repository is recommended for developers.The code is downloaded as follows:

    git clone https://github.com/fruitflybrain/ffbo.neuroarch_component

Building and running the repository is simplified with Docker Compose, which stores the configuration for a service (such as network and name for the container and the Dockerfile to build from) in a docker-compose.yml file, simplifying the command-line call. Building and running the Docker image can be accomplished with:

    docker-compose build
    docker-compose run

Note that the container can be both built and run with the following command:

    docker-compose up

### Manual Execution

Downloading and building the repository and image are accomplished the same as in the above section. Accessing the bash interface for the container can be accomplished with:

    docker run -P -it --net ffbonet --name ffbo.neuroarch_component fruitflybrain/neuroarch_component:local bash

Running the database is done with:

    sh run_component_docker.sh ws://ffbo.processor:8081/ws --no-ssl

or

    export PYTHONPATH=/neuroarch:/usr/local/lib/python2.7/site-packages:/usr/lib/python2.7/dist-packages/:$PYTHONPATH
    sh /opt/orientdb/bin/server.sh &
    sleep 25
    python neuroarch_component.py --url ws://ffbo.processor:8081/ws --no-ssl

This will start up the database and launch the NeuroArch component.

### Configuration

FFBO components are configured using .ini files. If you are building and running Docker images on your local computer from the Github repository (including Docker Compose), you can configure the NLP Component via the './config.ini' file in the main directory of this repository. However, if you are downloading images directly from Docker Hub, you will need to create a '.ffbolab' folder in your computer's home directory. Into this directory, place a .ini config file referring to this component. This can be done in one of two ways. Either copy the default config file from the main directory of this repository via:

    cp config.ini ~/.ffbo/config/ffbo.neuroarch_component.ini

or, in the case that you don't have this repository installed, via:

    wget -o ~/.ffbo/config/ffbo.neuroarch_component.ini https://cdn.rawgit.com/fruitflybrain/ffbo.neuroarch_component/master/config.ini

Once you have configured the .ini file, you can run it with:

    docker run -P -it --net ffbonet --name ffbo.neuroarch_component -v ~/.ffbo/config:/config fruitflybrain/ffbo.neuroarch_component:local

Or equivalently for other build methods. If you have configured a port, make sure to expose it by adding the '-p [INTERNAL PORT]:[EXTERNAL PORT]', where the internal port is the port you configured in the .ini file and the external port is the port on localhost that the output of the internal port is mapped to. Running without docker is the same process described above in the Manual Execution section.

