import logging
import json
import os

headers = json.loads(os.getenv("REQUIRED_HEADERS"))
logger = logging.getLogger(__name__)

# metadata tags
PRIMARY_ID = "External-Identifier"
UUID_ID = "Internal-Sender-Identifier"
CONTACT = "Contact-Name"
EXTERNAL_DESCRIPTION = "External-Description"