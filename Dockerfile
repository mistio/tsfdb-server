FROM python:3-buster

ARG FDB_CLIENT_DEB=https://www.foundationdb.org/downloads/6.2.7/ubuntu/installers/foundationdb-clients_6.2.7-1_amd64.deb
ENV FDB_CLIENT_DEB=${FDB_CLIENT_DEB}

RUN apt-get update && \
    apt-get install -y curl procps && \
    curl ${FDB_CLIENT_DEB} -o foundationdb-clients.deb && \
    dpkg -i foundationdb-clients.deb && \
    rm foundationdb-clients.deb && \
    rm -r /var/lib/apt/lists/* && \
    mkdir -p /usr/src/app

WORKDIR /usr/src/app

COPY requirements.txt /usr/src/app/
COPY setup.py /usr/src/app/

RUN pip3 install --no-cache-dir -r requirements.txt && \
    pip3 install uwsgi ipython && \
    pip3 install -e /usr/src/app

COPY . /usr/src/app

EXPOSE 8080

ENTRYPOINT ["python3"]

CMD ["-m", "tsfdb_server_v1"]