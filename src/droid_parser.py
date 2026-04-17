import pandas as pd
import logging
import os


class DroidReport:
    """Wrapper for a DataFrame to filter and adjust content"""

    def __init__(self, df: pd.DataFrame, current_dir: str):
        self.errors = []
        self.root_path = df.iloc[0].FILE_PATH
        self.current_dir = current_dir
        self.df = self._tidy_data(df)

    def check_droid_headers(self) -> bool:
        expected = [
            "ID",
            "PARENT_ID",
            "URI",
            "FILE_PATH",
            "NAME",
            "METHOD",
            "STATUS",
            "SIZE",
            "TYPE",
            "EXT",
            "LAST_MODIFIED",
            "EXTENSION_MISMATCH",
            "MD5_HASH",
        ]
        missing = []
        for c in expected:
            if c not in self.df.columns:
                missing.append(c)
        if len(missing) > 0:
            logging.warning(f"Headers not matched in report: {';'.join(missing)}")
            return False
        else:
            return True

    def _find_folder_path(self, example_file):
        original_path = self.root_path
        while True:
            new_path = example_file.replace(self.root_path, self.current_dir)
            if os.path.exists(new_path):
                return original_path
            elif len(original_path) <= 3:
                print(f"Unable to find file {example_file} in current location.")
                raise ValueError(
                    f"Unable to find file {example_file} in current location."
                )
            else:
                original_path = os.path.normpath(
                    original_path.replace(os.path.basename(original_path), "")
                )

    def _add_whitespace_to_name(self, df) -> pd.DataFrame:
        name = df.FILE_PATH
        name = name.apply(os.path.basename)
        df.NAME = name
        return df

    def _tidy_data(self, df) -> pd.DataFrame:
        # drop folders
        files_only = df[df["TYPE"] != "Folder"]
        rows = len(files_only.index)
        logging.debug(
            f"Dropping folders - reducing manifest size from {len(df.index)} to {rows}, "
        )
        # drop items inside archive formats
        files_only = files_only[files_only["URI"].str.startswith("file")]
        logging.debug(
            f"Dropping files within archive formats - reducing manifest size from {rows} to {len(files_only.index)}"
        )
        example_file = files_only.iloc[0].FILE_PATH

        try:
            to_replace = self._find_folder_path(example_file)
        except Exception as e:
            self.errors.append(f"Error: {e} - have files been renamed?")
            return df
        files_only.loc[:, "CHECKED_PATH"] = files_only.loc[:, "FILE_PATH"].str.replace(
            to_replace, self.current_dir
        )

        files_only = self._add_whitespace_to_name(files_only)
        return files_only


class DroidCsvParser:
    """Loads DROID reports from CSV even when they have
    irregular headers due to format report weirdness"""

    def _handle_irregular_csv(self, file):
        # this is a bit hackey :(
        with open(file, "r", encoding="utf-8") as f:
            longest = 0
            # split assuming we have quoated data and commas
            data = [line.split('","') for line in f]

            # find the longest line
            for line in data:
                if len(line) > longest:
                    longest = len(line)

            # tidy column names
            col_names = [x.replace('"', "").replace("\n", "") for x in data[0]]

            # add column names for extra columns
            for i in range(len(col_names), longest):
                col_names.append(f"Column {i+1}")
            data[0] = col_names

            # add blank entries for any other short columns
            for i in range(len(data)):
                if len(data[i]) < longest:
                    to_add = [""] * (longest - len(data[i]))
                    data[i].extend(to_add)
            col_names = data[0]
            data = data[1:]

            # create indexed dictionary and load dataframe
            total = {}
            for i in range(longest):
                total.update({str(i): data[i]})
            df = pd.DataFrame.from_dict(total, orient="index", columns=col_names)
            return df

    def _load_csv(self, csv_file):
        readerror = False
        logging.info(f"Checking report: {csv_file}")
        try:
            df = pd.read_csv(csv_file)
        except Exception as e:
            logging.error(f"Error parsing as dataframe: {e}")
            readerror = True
            # continue

        if readerror:
            df = self._handle_irregular_csv(csv_file)

        return df

    def get_droid_report(self, csv_path: str, current_dir) -> DroidReport:
        df = self._load_csv(csv_path)
        dr = DroidReport(df, current_dir)
        return dr
