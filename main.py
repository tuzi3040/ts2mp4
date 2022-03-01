from google.cloud import storage
from google.cloud import exceptions as gcp_exceptions
import functions_framework
import subprocess
import base64
import json
import re
import os

@functions_framework.cloud_event
def main(cloud_event):
    print(f'''Function `ts2mp4` triggered by:
        TIME: {cloud_event["time"]}
        MESSAGE_ID: {cloud_event["id"]}
        SOURCE: {cloud_event["source"]}''')

    message_obj = json.loads(base64.b64decode(cloud_event.data["message"]["data"]).decode())
    from_gs_url = message_obj["from_gs_url"]
    to_bucket = message_obj["to_bucket"]
    print(f'FROM: {from_gs_url}\nTO: {to_bucket}')

    gs_url_match = re.match(r'^gs://(?P<from_bucket>[0-9a-z_.-]{3,222})/(?P<from_blob_path>.+\.ts)$', from_gs_url)
    if not gs_url_match:
        raise ValueError(f'`{from_gs_url}` not match /^gs://(?P<from_bucket>[0-9a-z_.-]{3,222})/(?P<from_blob_path>.+\.ts)$/.')
    from_bucket = gs_url_match.group("from_bucket")
    from_blob_path = gs_url_match.group("from_blob_path")
    from_filename = from_blob_path.split('/')[-1]
    to_blob_path = re.sub('ts$', 'mp4', from_blob_path)
    to_filename = re.sub('ts$', 'mp4', from_filename)

    storage_client = storage.Client()
    from_bucket_instance = storage_client.get_bucket(from_bucket)
    to_bucket_instance = storage_client.get_bucket(to_bucket)

    from_blob_instance = from_bucket_instance.get_blob(from_blob_path)
    if not from_bucket_instance:
        raise gcp_exceptions.NotFound(f'Blob `{from_gs_url}` not found.')
    print(f'Blob `{from_gs_url}` size: {from_blob_instance.size}')
    if from_blob_instance.size > 8_053_063_680:
        raise NotImplementedError('Blob too large.')

    tmp_from_file = os.path.join('/tmp', from_filename)
    print(f'Downloading blob `{from_gs_url}`.')
    from_blob_instance.download_to_filename(tmp_from_file, raw_download = True)
    print(f'Blob downloaded to `{tmp_from_file}`.')

    tmp_to_file = os.path.join('/tmp', to_filename)
    print("Invoking ffmpeg...")
    ffmpeg_ps = subprocess.run(['ffmpeg', '-i', tmp_from_file, '-c', 'copy', tmp_to_file], capture_output = True)
    print(f'{" ".join(ffmpeg_ps.args)}\n{ffmpeg_ps.stderr}')
    ffmpeg_ps.check_returncode()

    to_blob_instance = to_bucket_instance.blob(to_blob_path)
    if to_blob_instance.exists():
        print(f'Blob gs://{to_bucket}/{to_blob_path} already exists. Overwriting...')
    print(f'Uploading blob from `{tmp_to_file}`.')
    to_blob_instance.upload_from_filename(tmp_to_file)
    print(f'Blob uploaded to `gs://{to_bucket}/{to_blob_path}`.')

    os.remove(tmp_from_file)
    print(f'`{tmp_from_file}` removed.')
    os.remove(tmp_to_file)
    print(f'`{tmp_to_file}` removed.')
