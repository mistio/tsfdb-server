import fdb
DO_NOT_CACHE_FDB_DIRS = False


def create_or_open_fdb_dir(db, fdb_dir, cached_dirs=None):
    if not DO_NOT_CACHE_FDB_DIRS and cached_dirs:
        current_dir = {"root": [cached_dirs, fdb.directory]}
        parent_key = "root"
        for key in fdb_dir:
            if not current_dir[parent_key][0].get(key):
                current_dir[parent_key][0][key] = \
                    {key: [{},
                           current_dir[parent_key][1].create_or_open(
                        db, (key,))]}
            current_dir = current_dir[parent_key][0][key]
            parent_key = key
        return current_dir[parent_key][1]
    else:
        return fdb.directory.create_or_open(db, fdb_dir)


def create_or_open_metric_dir(db, fdb_dir, cached_dirs=None):
    parent_dir = create_or_open_fdb_dir(db, fdb_dir[:-1], cached_dirs)
    parent_dir.create_or_open(db, (fdb_dir[-1],))
