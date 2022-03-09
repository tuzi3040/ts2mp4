from google.cloud import storage
from google.cloud import exceptions as gcp_exceptions
import functions_framework
import subprocess
import base64
import json
import re
import os

from gcp_logentry_logging import gcp_logentry_logging

@functions_framework.cloud_event
def main(cloud_event):
    message_obj = json.loads(base64.b64decode(cloud_event.data["message"]["data"]).decode())
    from_gs_url = message_obj["from_gs_url"]
    to_bucket = message_obj["to_bucket"]
    
    gcp_logentry_logger = gcp_logentry_logging(from_gs_url, to_bucket)
    
    gcp_logentry_logger.debug(f'FROM: {from_gs_url}\nTO: {to_bucket}')
    gcp_logentry_logger.debug(f'''Function `ts2mp4` triggered by:
        TIME: {cloud_event["time"]}
        MESSAGE_ID: {cloud_event["id"]}
        SOURCE: {cloud_event["source"]}''')

    from_gs_url_pattern = r'^gs://(?P<from_bucket>[0-9a-z_.-]{3,222})/(?P<from_blob_path>.+\.ts)$'
    gs_url_match = re.match(from_gs_url_pattern, from_gs_url)
    if not gs_url_match:
        raise ValueError(f'`{from_gs_url}` not match /{from_gs_url_pattern}/.')
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
    gcp_logentry_logger.info(f'Blob `{from_gs_url}` size: {from_blob_instance.size}')
    if from_blob_instance.size > 8_053_063_680:
        raise NotImplementedError('Blob too large.')

    tmp_from_file = os.path.join('/tmp', from_filename)
    gcp_logentry_logger.info(f'Downloading blob `{from_gs_url}`.')
    from_blob_instance.download_to_filename(tmp_from_file, raw_download = True)
    gcp_logentry_logger.notice(f'Blob downloaded to `{tmp_from_file}`.')

    tmp_to_file = os.path.join('/tmp', to_filename)
    gcp_logentry_logger.info("Invoking ffmpeg...")
    ffmpeg_ps = subprocess.run(['ffmpeg', '-i', tmp_from_file, '-c', 'copy', tmp_to_file], capture_output = True)
    gcp_logentry_logger.notice(f'{" ".join(ffmpeg_ps.args)}\n{ffmpeg_ps.stderr}')
    ffmpeg_ps.check_returncode()

    to_blob_instance = to_bucket_instance.blob(to_blob_path)
    if to_blob_instance.exists():
        gcp_logentry_logger.warning(f'Blob gs://{to_bucket}/{to_blob_path} already exists. Overwriting...')
    gcp_logentry_logger.info(f'Uploading blob from `{tmp_to_file}`.')
    to_blob_instance.upload_from_filename(tmp_to_file)
    gcp_logentry_logger.notice(f'Blob uploaded to `gs://{to_bucket}/{to_blob_path}`.')

    os.remove(tmp_from_file)
    gcp_logentry_logger.info(f'`{tmp_from_file}` removed.')
    os.remove(tmp_to_file)
    gcp_logentry_logger.info(f'`{tmp_to_file}` removed.')
