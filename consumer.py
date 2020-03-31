from time import sleep

from tsfdb_server_v1.controllers.db import open_db, write_in_kv
from tsfdb_server_v1.controllers.queue import Queue, Subspace


def main():
    db = open_db()
    queue = Queue(Subspace(('queue',)))
    max_bytes = 10000/2
    while True:
        data = queue.pop(db)
        if data:
            # Create a list of lines
            data = data.split('\n')
            # Get rid of all empty lines
            data = [line for line in data if line != ""]
            current_bytes = 0
            data_to_write = ""
            for line in data:
                current_bytes += len(line.encode('utf-8'))
                data_to_write += line + '\n'
                if current_bytes >= max_bytes:
                    write_in_kv(data_to_write)
                    data_to_write = ""
                    current_bytes = 0
            if current_bytes > 0:
                write_in_kv(data_to_write)
        else:
            sleep(1)


if __name__ == "__main__":
    main()
