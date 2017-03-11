###########################################
# run using: 
# docker build -t ffbo/neuroarch:develop .
# docker run -it ffbo/neuroarch:develop sh run_component.sh ws://processor_ip:8080/ws
###########################################
# Incomplete, need to clone neuroarch and neurarch_component repos

FROM ffbo/neuroarch

# File Author / Maintainer
MAINTAINER Adam Tomkins <a.tomkins@sheffield.ac.uk>

# Clone the conf files into the docker container
WORKDIR /app/neuroarch_component
RUN git fetch && git checkout develop

RUN git pull
RUN python setup.py install

WORKDIR /app/neuroarch
RUN git fetch && git checkout master
RUN git pull
RUN python setup.py install

WORKDIR /app/pyorient
RUN git pull

WORKDIR /app/neuroarch_component/neuroarch_component

CMD sh run_component.sh ws://localhost:8080/ws
