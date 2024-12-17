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

"""Identifier parsing patterns"""
# Assumes the identifiers will be final value or followed by " ", "_" or "-".
final = "(?=[_-]|\\s|$)"
# Assumes identifier parts will be separated by "-", "." or "_".
sep = "[\\-\\._]?\\s?"
IDENTIFIER_REGEX = (
    # SC numbers
    f"(SC{sep}\\d{{4,}}{final})|"
    # RA numbers
    f"(RA{sep}\\d{{4}}{sep}\\d+{final})|"
    # PA numbers
    f"(PA{sep}\\d\\d)(\\d\\d)?({sep}\\d+{final})|"
    # MS numbers
    f"(MS{sep}\\d{{2,}}{final})|"
    # capture case for PO numbers that should fail validation
    f"(PO{sep}\\d+{sep}slvdb){final}|"
    # Legacy purchase orders
    f"(POL{sep})?(\\d{{3,}}{sep}slvdb){final}|"
    # Current purchase orders
    f"(POL{sep}\\d{{3,}}{final})|"
    # H numbers
    f"(H\\d\\d)(\\d\\d)?({sep}\\d+){final}"
)
VALIDATION_REGEX = r"SC\d{4,}|RA-\d{4}-\d+|PA-\d{2}-\d+|MS-?\d{2,}|POL-\d{3,}|(POL-)?\d{3,}-slvdb|H\d{2}(\d\d)?-\d+"
