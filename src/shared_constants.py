import logging
from datetime import datetime
import json
import sys
import os

# headers = json.loads(os.getenv("REQUIRED_HEADERS"))
logger = logging.getLogger(__name__)

# running filename
RUNNING = ".running0"

# metadata tags
PRIMARY_ID = "External-Identifier"
UUID_ID = "Internal-Sender-Identifier"
CONTACT = "Contact-Name"
EXTERNAL_DESCRIPTION = "External-Description"
SOURCE_ORGANIZATION = "Source-Organization"
BAGGING_DATE = "Bagging-Date"
