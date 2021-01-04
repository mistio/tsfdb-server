FROM python:3.8.2-buster

ARG FDB_CLIENT_DEB=https://www.foundationdb.org/downloads/6.2.20/ubuntu/installers/foundationdb-clients_6.2.20-1_amd64.deb
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
    pip3 install --no-cache-dir uwsgi ipython && \
    pip3 install --no-cache-dir -e /usr/src/app

COPY . /usr/src/app

EXPOSE 8080

ENTRYPOINT [ "uwsgi" ]

CMD ["--plugins", "python3", "--http", "0.0.0.0:8080", "--wsgi-file", "tsfdb_server_v1/__main__.py", "--callable", "application", "--master", "--processes", "32", "--max-requests", "100"]
