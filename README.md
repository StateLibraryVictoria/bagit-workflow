# stash-it

## Overview

This is a basic workflow for stashing data safely on a server with minimum metadata as BagIt bags. Includes a record of transfers using a sqlite2 database.

## Getting started

### Dependencies

- [bagit](https://github.com/LibraryOfCongress/bagit-python)
- [Pytest](https://docs.pytest.org/en/stable/)

### Staging script

This process is designed to use minimal metadata submitted as JSON. The current configuration looks for a `folder_title.ok` file that contains minimum metadata. 

To generate these files, a Windows Batch file can be created to generate a `folder_title.ready` file with minimum metadata fields. Once the metadata is updated, the user renames the file to `.ok`.

Example `.bat` script:

        @echo off
        for /D %%i in (*) do if not exist %%i.ok (
            if not exist %%i.error (
                set var=%%i
                call :metadata %var% >%%i.ready))

        :metadata
        echo {
        echo     "record-set": "add classification",
        echo     "Source-Organization":"",
        echo     "Contact-Name":"",
        echo     "External-Description":"",
        echo     "Internal-Sender-Identifier": "%var%",
        echo }


### Metadata model

To do:
- Outline the model

### Testing

To do:
- Outline the tests

## Development

Features to add:
- Trigger
- Metadata parsing
- Bagging process
- Copying process
- Database recording
- Logging
