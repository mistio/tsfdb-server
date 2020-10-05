#!/usr/bin/env python3

import connexion

from tsfdb_server_v1 import encoder


app = connexion.App(__name__, specification_dir='./openapi/')
app.app.json_encoder = encoder.JSONEncoder
app.add_api('openapi.yaml',
            arguments={'title': 'TSFDB'},
            pythonic_params=True)
application = app.app


if __name__ == '__main__':
    app.run(port=8080)
