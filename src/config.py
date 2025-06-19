from src.id_parser import IdParser
import logging
import json

class Config:

    def __init__(self, target_file):
        self.file = target_file
        self.id_parser = None
        self.config = self.load_json_configs()

    def load_json_configs(self):
        with open(self.file, 'r') as f:
            self.config = json.load(f)

        self.parse_configs()

    def parse_configs(self):
        id_parser = self.config["id_parser"]
        validation_patterns = id_parser.get("validation_patterns")
        logging.debug("Loaded validation patterns: %s", validation_patterns)
        sep = id_parser.get("sep")
        logging.debug("Loaded separator: %s", sep)
        identifier_patterns = id_parser.get("identifier_patterns")
        logging.debug("Loaded identifier patterns: %s", identifier_patterns)
        ## validation patterns should be a list of OR patterns
        validation_pattern = "|".join(validation_patterns)
        ## identifier_patterns may have separators configured using "sep"
        if sep is not None:
            identifier_pattern = "|".join([sep.join(x) for x in identifier_patterns])
        else:
            identifier_pattern = "|".join(identifier_patterns)

        ## normalisation patterns are used to fix common identifiers written in different ways
        normalisation_patterns = id_parser.get("normalisation_tests")
        normalisation_joins = id_parser.get("normalisation_joins")
        logging.debug("Normalisation patterns: %s", normalisation_patterns)
        logging.debug("Normalisation joinse: %s", normalisation_joins)

        logging.debug("Validation pattern: %s", validation_pattern)
        logging.debug("Identifier pattern: %s", identifier_pattern)
        self.id_parser = IdParser(validation_pattern, identifier_pattern, normalisation_patterns, normalisation_joins)

    def get_id_parser(self):
        return self.id_parser



        
    