import os
import pandas as pd
import logging
import hashlib
from time import strftime
import json
import sys

transfer_dir = os.getenv("TRANSFER_DIR")
logging_dir = os.getenv("LOGGING_DIR")
output_directory = os.getenv("REPORT_DIR")
droid_output_dir = os.getenv("DROID_OUTPUT_DIR")

def getHash(path, root):
    hasher = hashlib.new('MD5')
    location = os.path.join(root,path)
    try:
        with open(location, "rb") as f:
            while True:
                block = f.read((512 * 1024))
                if not block:
                    break
                hasher.update(block)
        outcome = hasher.hexdigest()
    except Exception as e:
        outcome = f"Error getting hash: {e}"
    return outcome

def check_droid_headers(current_headers: list) -> bool:
    expected = ["ID","PARENT_ID","URI","FILE_PATH","NAME","METHOD","STATUS","SIZE","TYPE","EXT","LAST_MODIFIED","EXTENSION_MISMATCH","MD5_HASH"]
    missing = []
    for c in expected:
                if c not in current_headers:
                    missing.append(c)
    if len(missing) > 0:
        logging.warning(f"Headers not matched in report: {';'.join(missing)}")
        return False
    else:
        return True

def find_folder_path(original_path, example_file, current_directory):
    while (True):
        new_path = example_file.replace(original_path,current_directory)
        if os.path.exists(new_path):
            return original_path
        elif len(original_path) <= 3:
            print(f"Unable to find file {example_file} in current location.")
            raise ValueError(f"Unable to find file {example_file} in current location.")
        else:
            original_path = os.path.normpath(original_path.replace(os.path.basename(original_path),""))

def make_error_file(directory, message="Error validating DROID report. Check the logs for more information."):
    print("Making error file")
    if not os.path.exists(directory):
        logging.warning(f"Doesn't exist {directory}")
        return 
    if not os.path.exists(transfer_dir):
        logging.warning(f"Doesn't exist {directory}")
        return
    try: 
        os.path.commonprefix([transfer_dir, directory])
        d = os.path.relpath(directory,transfer_dir)
        top_level = os.path.basename(d)
        ok_path = os.path.join(transfer_dir,f"{top_level}.ok")
        ready_path = os.path.join(transfer_dir,f"{top_level}.ready")
        error_path = os.path.join(transfer_dir,f"{top_level}.error")
        with open(error_path, 'w') as f:
            f.write(message)
            logging.info("Creating error file for transfer.")
        if os.path.exists(ok_path):
            os.remove(ok_path)
            logging.info("Removing .ok file for folder with errors.")
        if os.path.exists(ready_path):
            os.remove(ready_path)
            logging.info("Removing .ready file for folder with errors.")
    except ValueError as e:
        logging.info(f"Path: {directory} and {transfer_dir} have no common path.")

def make_ok_file(directory):
    try: 
        os.path.commonprefix([transfer_dir, directory])
        d = os.path.relpath(directory,transfer_dir)
        top_level = os.path.basename(d)
        ok_path = os.path.join(transfer_dir,f"{top_level}.ok")
        ready_path = os.path.join(transfer_dir,f"{top_level}.ready")
        with open(ok_path, 'w') as f:
            f.write("DROID report validated.")
            logging.info("Updating ready file to ok file for transfer.")
        if os.path.exists(ready_path):
            os.remove(ready_path)
            logging.info("Removing .ready file for validated folder.")
    except ValueError as e:
        logging.error(f"Error making ok file: {e}")
        make_error_file(directory)
    

def handle_irregular_csv(file):
    # this is a bit hackey :(
    with open(file, 'r', encoding='utf-8') as f:
        longest = 0
        # split assuming we have quoated data and commas
        data = [line.split('","') for line in f]

        # find the longest line
        for line in data:
            if len(line) > longest:
                longest = len(line)

        # tidy column names
        col_names = [x.replace('"',"").replace("\n","") for x in data[0]]

        # add column names for extra columns
        for i in range(len(col_names),longest):
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
            total.update({str(i):data[i]})
        df = pd.DataFrame.from_dict(total, orient="index", columns=col_names)
        return df

def load_directories(dir):
    if dir is None:
        logging.error("No transfer directory recorded.")
        sys.exit()

    # process each folder within a directory supplied as path
    paths = os.listdir(dir)
    ready_files = [x.replace(".ready","") for x in paths if x.endswith(".ready")]
    logging.info("Processing READY files: " + ", ".join(ready_files))
    if len(ready_files) == 0:
        logging.info("No staged folders to process. Exiting...")
        sys.exit()
    just_ready = [x for x in paths if x in ready_files]
    full_paths = [os.path.join(dir, x) for x in just_ready]
    directories = [x for x in full_paths if os.path.isdir(x)]
    logging.info("Processing directories: " + ", ".join(directories))
    dir_list = [os.path.join(dir, x) for x in just_ready]
    logging.info("Final list of directories to be processed: " + ", ".join(dir_list))
    return dir_list

def load_csv(csv_file):
    readerror = False
    logging.info(f"Checking report: {csv_file}")
    try:
        df = pd.read_csv(csv_file)
    except Exception as e:
        logging.error(f"Error parsing as dataframe: {e}")
        readerror = True
        #continue
    
    if readerror:
        df = handle_irregular_csv(csv_file)

    return df

def main():
    
    logfilename = f"{strftime('%Y%m%d')}_check_DROID_report.log"
    logfile = os.path.join(logging_dir, logfilename)

    logging.basicConfig(
        filename=logfile,
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    # check the variables are loaded

    report_dir = os.path.join(output_directory,"droid_report_check")
    if not os.path.exists(report_dir):
        os.mkdir(report_dir)

    dir_list = load_directories(transfer_dir)

    for dir in dir_list:
        logging.info("==Starting process==")
        logging.info(f"Processing directory: {dir}")
        files = os.listdir(dir)
        errors = []

        droid_report = [x for x in files if 'droid' in x.lower()]
        logging.info(droid_report)
        if len(droid_report) == 0:
            logging.warning(f"No droid report in {dir}")
            errors.append("No droid report in folder.")

        validated_reports = []

        for r in droid_report:
            logging.info(f"Processing droid report: {r}")
            csv_file = os.path.join(dir,r)
            df = load_csv(csv_file)
            
            # validate as a DROID report by checking expected headers
            cols = df.head()
            if not check_droid_headers(cols):
                continue

            # DROID report store absolute paths at time of generation
            # script tries to find a legit path to the files
            root_path = df.iloc[0].FILE_PATH
            # drop folders
            files_only = df[df["TYPE"]!="Folder"]
            rows = len(files_only.index)
            logging.info(f"Dropping folders - reducing manifest size from {len(df.index)} to {rows}, ")
            # drop items inside archive formats
            files_only = files_only[files_only["URI"].str.startswith("file")]
            logging.info(f"Dropping files within archive formats - reducing manifest size from {rows} to {len(files_only.index)}")
            example_file = files_only.iloc[0].FILE_PATH

            try:
                to_replace = find_folder_path(root_path, example_file, dir)
            except Exception as e:
                errors.append(f"Error: {e} - have files been renamed?")
                logging.error(f"Error: {e} - have files been renamed?")
                continue
            name = os.path.basename(os.path.normpath(dir))
            files_only.loc[:,'CHECKED_PATH'] = files_only.loc[:,"FILE_PATH"].str.replace(to_replace,dir)

            fdf = files_only

            # generate hashes of files in current locations
            fdf['CURRENT_MD5'] = fdf['CHECKED_PATH'].apply(lambda x: getHash(x, dir))

            # compare generated hashes against existing
            fdf['STILL_VALID'] = (fdf['MD5_HASH']==fdf['CURRENT_MD5'])

            # filter to relevant fields for report
            df2 = fdf.loc[:,['STILL_VALID','FILE_PATH', 'MD5_HASH', 'CURRENT_MD5', 'CHECKED_PATH']]
            df2_error = df2[df2['STILL_VALID']==False]

            # generate reports and write to file.
            name, ext = os.path.splitext(r)
            all_file = os.path.join(report_dir, f'{name}_all_{strftime("%Y-%m-%d")}.csv')
            df2.to_csv(all_file)
            print("Report written to: " + all_file)
            if len(df2_error) > 0:
                df2_read_error = df2_error[df2_error['CURRENT_MD5'].str.startswith("Error")]
                df2_match_error = df2_error[~df2_error['CURRENT_MD5'].str.startswith("Error")]
                read_error_file = os.path.join(report_dir, f'{name}_error_read_{strftime("%Y-%m-%d")}.csv')
                match_error_file = os.path.join(report_dir, f'{name}_error_match_{strftime("%Y-%m-%d")}.csv')
                if (len(df2_read_error) > 0):
                    df2_read_error.to_csv(read_error_file)
                    logging.warning(f"Read errors identified.")
                    errors.append("Read errors identified.")
                else:
                    logging.warning("No read errors identified")
                if (len(df2_match_error) > 0):
                    df2_match_error.to_csv(match_error_file)
                    logging.warning("File match errors identified.")
                    errors.append("File match errors identified")
                else:
                    logging.info("No match errors identified")
                make_error_file(dir, ", ".join(errors))
            else:
                make_ok_file(dir)
                logging.info("====SUCCESS: Data matches report!====")
                logging.info("Moving DROID report to done folder.")
                move_time = strftime("%Y%m%d%H%M%S")
                out_dir = os.path.join(droid_output_dir, move_time)
                if not os.path.exists(out_dir):
                    os.mkdir(out_dir)
                    increment = 0
                else:
                    increment = len(os.listdir(out_dir))
                out_dir = os.path.join(out_dir,f"{increment}")
                os.mkdir(out_dir)
                os.rename(csv_file, os.path.join(out_dir,r))
                print(f"Moved {csv_file} to {os.path.join(out_dir,r)}")

            if len(errors) == 0:
                validated_reports.append(r)
            
        if len(validated_reports) == 0:
            make_error_file(dir, ", ".join(errors))
        for report in validated_reports:
            droid_report.remove(report)
        
        logging.info(f"Files successfully processed:")
        if len(validated_reports) > 0:
            print(f"Files successfully processed:")
            for r in validated_reports:
                logging.info(r)
                print(r)
        if len(droid_report) > 0:
            logging.info(f"Files not recognised as DROID reports:")
            print(f"Files not recognised as DROID reports:")
            for f in droid_report:
                logging.info(f)
                print(f)

if __name__ == "__main__":
    main()