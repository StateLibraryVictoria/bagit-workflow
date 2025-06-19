
import re
import logging

class IdParser:
    """Parser for pulling identifiers from strings based on config.

    Keyword arguments:
        valid_pattern -- regex pattern for validating identifiers
        pattern_list -- list of regex with identifier patterns
    """

    def __init__(self, valid_pattern: str, pattern_list: str):
        self
        self.valid_pattern = valid_pattern
        self.pattern_list = pattern_list

    def validate_id(self, id: str) -> bool:
        """Returns True if submitted data matches valid ID pattern"""
        if id is None:
            return False
        return re.fullmatch(self.valid_pattern, id) is not None

    def normalise_id(
        self,
        id: str,
        default_regex: str = "[_\.]|\s",
        replace_with: str = "-",
        tests: list = [r"(MS)(\d+)", r"(SC)\D?(\d+)"],
        join_by: list = ["-", ""],
    ) -> str:
        """Normalise based on patterns for easy searching.
        Defaults to replacing underscores, periods and whitespace with hyphens.
        Includes optional list of regex tests with matchgroups and delimiters to
        address specific issues.

        Keyword arguments:
        id -- identifier to be normalised
        default_regex -- matching patterns will be replaced (default "[_\.]|\s")
        replace_with -- what default_regex is replaced with (default "-")
        tests -- patterns with capturing groups to match and rejoin (default [r"(MS)(\d+)",r"(SC)\D?(\d+)"])
        join_by -- correlating delimiters for joins (default ["-",""])
        """
        # check if id is a MS number.
        norm_id = norm_id = re.sub(default_regex, replace_with, id)
        if tests is not None and join_by is not None and ((len(tests) == len(join_by))):
            for i in range(len(tests)):
                results = re.match(tests[i], id)
                if results is not None:
                    norm_id = join_by[i].join(results.groups())
        if norm_id != id:
            logging.info(f"Normalised id {id} to {norm_id}")
        return norm_id

    def get_ids(self, string: str, normalise: bool = True) -> list | None:
        """Finds identifiers in strings based on supplied identifier pattern regex.
        Returns ids as a list which is compatible with Python BagIt.
        If no ids can be parsed, returns None.

        Keyword arguments:
        string -- String to be searched for identifiers.
        normalise -- Option to set identifiers to normalised values before returning. (default True)
        """

        matches = re.findall(self.pattern_list, string)
        if len(matches) < 1:
            ids = None
        else:
            ids = []
            for match in matches:
                id = "".join((match))
                if normalise:
                    id = self.normalise_id(id)
                if self.validate_id(id):
                    ids.append(id)
        return ids
