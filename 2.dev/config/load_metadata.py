import sys
import json
from google.cloud import datastore


def load_meta_data (project_name, metadata_file, kind_name):
    with open(metadata_file) as f:
        metadata = json.load(f)

    client = datastore.Client(project=project_name)
    for val in metadata:
        key = client.key(kind_name, val['PipeLine']+"-"+str(val['SequenceID']))
        entity = datastore.Entity(key=key)
        entity.update(val)
        client.put(entity)
        print('Inserted data into Datastore')


if __name__ == "__main__":  # pragma: no cover
    project = sys.argv[1] #  'python-dfl'
    file_name = 'config/datastore_metadata_sbx.json'
    kind = 'FrameworkMetaData-TFTest'
    load_meta_data(project, file_name, kind)
