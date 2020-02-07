FROM python:3-buster

ARG FDB_CLIENT_DEB=https://www.foundationdb.org/downloads/6.2.11/ubuntu/installers/foundationdb-clients_6.2.11-1_amd64.deb
ENV FDB_CLIENT_DEB=${FDB_CLIENT_DEB}
ENV AGGREGATE_MINUTE=1
ENV AGGREGATE_HOUR=2
ENV AGGREGATE_DAY=2

RUN apt-get update && \
    apt-get install -y curl procps && \
    curl ${FDB_CLIENT_DEB} -o foundationdb-clients.deb && \
    dpkg -i foundationdb-clients.deb && \
    rm foundationdb-clients.deb && \
    rm -r /var/lib/apt/lists/* && \
    mkdir -p /usr/src/app

WORKDIR /usr/src/app

COPY test-requirements.txt /usr/src/app/
COPY setup.py /usr/src/app/

RUN pip3 install --no-cache-dir -r test-requirements.txt && \
    pip3 install ipython && \
    pip3 install -e /usr/src/app

COPY . /usr/src/app

CMD ["python", "tsfdb_server_v1/test/test_datapoints_controller.py"]