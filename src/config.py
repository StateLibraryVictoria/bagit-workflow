from src.helper_functions import IdParser
import json

class Config:

    def __init__(self, target_file):
        self.file = target_file
        self.id_parser = []
        self.config = self.load_json_configs()

    def load_json_configs(self):
        with open(self.file, 'r') as f:
            self.config = json.load(f)

        self.parse_configs()

    def parse_configs(self):
        id_parser = self.config["id_parser"]
        validation_patterns = id_parser.get("validation_patterns")
        sep = id_parser.get("sep")
        identifier_patterns = id_parser.get("identifier_patterns")
        ## validation patterns should be a list of OR patterns
        validation_pattern = "|".join(validation_patterns)
        ## identifier_patterns may have separators configured using "sep"
        if sep is not None:
            identifier_pattern = "|".join([sep.join(x) for x in identifier_patterns])
        else:
            identifier_pattern = "|".join(identifier_patterns)
        self.id_parser.append(IdParser(validation_pattern, identifier_pattern))

    def get_id_parser(self, only_first=True):
        if only_first:
            return self.id_parser[0]
        return self.id_parser



        
    