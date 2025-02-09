import pandas as pd
from datetime import datetime

from src.database_functions import *
from src.helper_functions import *
from src.shared_constants import *


class ReportType(ABC):
    """
    Declares operations common to all supported reports.
    """

    @abstractmethod
    def build_basic_report(self, database, identifier):
        pass


class Report:
    """
    Defines Report interfaces.
    """

    def __init__(self, report_type: ReportType) -> None:
        self._report_type = report_type

    @property
    def report_type(self) -> ReportType:
        return self._report_type

    @report_type.setter
    def report_type(self, report_type: ReportType) -> None:
        self.report_type = report_type

    def build_basic_report(self, database: str, id: str = None) -> str:
        report = self._report_type.build_basic_report(database, id)
        return report


class ValidationReport(ReportType):
    """
    Generates validation reports.
    """

    def build_basic_report(self, validation_db, validation_action_id=None) -> str:
        if validation_action_id == None:
            query = "SELECT * from ValidationActions"
        else:
            query = f"SELECT * from ValidationActions WHERE ValidationActionsId={validation_action_id}"
        html_start = html_header("Validation Report")
        html_action = return_db_query_as_html(
            validation_db,
            query,
        )
        html_outcome = return_db_query_as_html(
            validation_db,
            query,
        )
        html_body = f"<body><h2>Report Overview</h2>{html_action}<h2>Validation Outcomes</h2>{html_outcome}</body></html>"
        return html_start + html_body


class TransferReport(ReportType):
    """
    Generates transfer reports
    """

    def build_basic_report(self, transfer_db, id=None):
        html_start = html_header("Transfer Report")
        html_body = "<body>"
        with get_db_connection(transfer_db) as con:
            df = pd.read_sql_query(f"SELECT * from transfers", con)
            df = self._tidy_transfer_df(df)
        html_body += "<h2>Records of transfer</h2>"
        html_body += df.to_html()
        html_body += "</body></html>"
        return html_start + html_body

    def _round_up_units(self, value):
        if int(value) < 1024:
            return f"{value} B"
        elif (int(value) / 1024) < 1024:
            return f"{(int(value) / 1024):.3f} KiB"
        elif (int(value) / (1024**2)) < 1024:
            return f"{(int(value) / (1024 ** 2)):.3f} MiB"
        else:
            return f"{(int(value) / (1024 ** 3)):.3f} GiB"

    def _tidy_transfer_df(self, dataframe):
        dataframe[["Bytes", "FileCount"]] = dataframe.PayloadOxum.str.split(
            ".", expand=True
        )
        dataframe.loc[:, "Size"] = dataframe.Bytes.apply(self._round_up_units)
        dataframe["StartTime"] = pd.to_datetime(dataframe.StartTime, format="ISO8601")
        dataframe["EndTime"] = pd.to_datetime(dataframe.EndTime, format="ISO8601")
        dataframe.loc[:, "TransferTimeSeconds"] = (
            dataframe.EndTime - dataframe.StartTime
        ).dt.total_seconds()
        col_order = [
            "TransferID",
            "TransferDate",
            "BagDate",
            "CollectionIdentifier",
            "Size",
            "FileCount",
            "OutcomeFolderTitle",
            "OriginalFolderTitle",
            "TransferTimeSeconds",
            "ContactName",
            "SourceOrganisation",
        ]
        dataframe = dataframe[
            col_order + [c for c in dataframe.columns if c not in col_order]
        ]
        return dataframe


def html_header(title: str):
    header = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="utf-8">
    <title>{title}</title>
    <style>
	body {{
	  font-family: "Andale Mono", monospace;
	}}
	</style>
</head>"""
    return header


def return_db_query_as_html(db_path: str, sql_query: str):
    with get_db_connection(db_path) as con:
        df = pd.read_sql_query(sql_query, con)
    return df.to_html()
