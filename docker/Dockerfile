###########################################
# run using: 
# docker build -t ffbo/neuroarch_component:develop .
# docker run --name neuroarch_component -v $(dirname `pwd`):/neuroarch_component -v $(dirname $(dirname `pwd`))/neuroarch:/neuroarch -it ffbo/neuroarch_component:develop sh /neuroarch_component/neuroarch_component/run_component_docker.sh
###########################################
FROM python:2

# File Author / Maintainer
MAINTAINER Adam Tomkins <a.tomkins@sheffield.ac.uk>
RUN apt-get update && apt-get install -y apt-transport-https

ENV HOME /app
ENV DEBIAN_FRONTEND noninteractive

# Add the application resources URL
RUN echo "deb http://archive.ubuntu.com/ubuntu/ trusty main universe" >> /etc/apt/sources.list
RUN apt-get update

# Install basic applications
RUN apt-get install -y --force-yes tar git curl vim wget dialog net-tools build-essential 

# Install Python and Basic Python Tools
RUN apt-get install -y --force-yes --force-yes python python-dev python-distribute python-pip

# Crossbar.io connection defaults
ENV CBURL ws://crossbar:8080/ws
ENV CBREALM realm1

# install Autobahn|Python
RUN pip install -U pip && pip install autobahn[twisted]

# Install Java
RUN apt-get install -y --force-yes default-jre

# Install OrientDB
RUN wget https://orientdb.com/download.php?file=orientdb-community-2.2.17.tar.gz
RUN tar -xf download.php?file=orientdb-community-2.2.17.tar.gz -C /opt
RUN mv /opt/orientdb-community-2.2.17 /opt/orientdb

# Install basic applications
RUN apt-get install -y --force-yes tar git curl vim wget dialog net-tools build-essential 


# Install dependancies
RUN pip install --upgrade pip
RUN pip install cython
RUN pip install simplejson

RUN pip install daff networkx path.py


RUN  apt-get -yq update && \
     apt-get -yqq install ssh
RUN apt-get install -y --force-yes python-h5py

RUN apt-get update


WORKDIR /opt/orientdb/databases
RUN wget -O ffbo_db.tar.gz https://goo.gl/FGJbPp
RUN tar zxvf ffbo_db.tar.gz
RUN mv ffbo_db na_server

# Package that supports binary serialization for pyorient
RUN pip install pyorient_native
RUN pip install pyOpenSSL
RUN pip install pandas
RUN pip install service_identity

# Install from forked pyorient till binary serialization support
# is integrated in the next release
WORKDIR /app
RUN git clone https://github.com/nikulukani/pyorient.git
WORKDIR /app/pyorient
RUN git fetch && git checkout develop
RUN python setup.py install

ENV ORIENTDB_ROOT_PASSWORD root
