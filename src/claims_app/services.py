"""The claims app's calls to Databricks: the damage model (serving endpoint) and claim photos (the claims volume)."""

import base64
import io
import os

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sdk.errors import NotFound

SERVING_ENDPOINT = os.environ["SERVING_ENDPOINT"]
# value_from a volume gives its full name (catalog.schema.volume); the Files API needs /Volumes/catalog/schema/volume.
_volume = os.environ["CLAIMS_VOLUME"]
VOLUME_PATH = _volume if _volume.startswith("/Volumes/") else "/Volumes/" + _volume.replace(".", "/")
UPLOAD_DIR = f"{VOLUME_PATH}/app_uploads"
# Where photos are: pipeline claims in images/ (or archive/, once Auto Loader moved them), app claims in app_uploads/.
PHOTO_DIRS = ["images", "archive", "app_uploads"]

_config = Config()
_workspace = WorkspaceClient()

# The endpoint scales to zero: the first request after idle can take about a minute while it starts.
FIRST_TIMEOUT_S = 30
RETRY_TIMEOUT_S = 120


def predict_damage(image: bytes) -> str | None:
    """'ok', 'minor' or 'major' from the part 5 model; None if the endpoint doesn't answer (the check then can't run)."""
    url = f"{_config.host.rstrip('/')}/serving-endpoints/{SERVING_ENDPOINT}/invocations"
    body = {"dataframe_records": [{"content": base64.b64encode(image).decode()}]}
    for timeout in (FIRST_TIMEOUT_S, RETRY_TIMEOUT_S):
        try:
            response = requests.post(url, headers=_config.authenticate(), json=body, timeout=timeout)
            response.raise_for_status()
            return response.json()["predictions"][0]
        except (requests.RequestException, KeyError, IndexError):
            continue
    return None


def save_photo(claim_no: str, image: bytes, extension: str) -> str:
    """Store an app claim's photo as app_uploads/<claim_no>.<extension>; returns the file name (image_name)."""
    image_name = f"{claim_no}.{extension}"
    _workspace.files.upload(f"{UPLOAD_DIR}/{image_name}", io.BytesIO(image), overwrite=True)
    return image_name


def read_photo(image_name: str) -> bytes | None:
    """A claim photo by file name, from images/, archive/ or app_uploads/; None if it isn't found."""
    for folder in PHOTO_DIRS:
        try:
            return _workspace.files.download(f"{VOLUME_PATH}/{folder}/{image_name}").contents.read()
        except NotFound:  # not in this folder: try the next one
            continue
    return None
