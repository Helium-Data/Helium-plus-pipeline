import os
from pathlib import Path
import paramiko
import gzip
import psycopg2
import config_heliumplus
import datetime

# SFTP server connection details
hostname = config_heliumplus.sftp_hostname
port = 22
username = config_heliumplus.sftp_username
password = config_heliumplus.sftp_password

# Connect to SFTP server
transport = paramiko.Transport((hostname, port))
transport.connect(username=username, password=password)
sftp = paramiko.SFTPClient.from_transport(transport)


def delete_all_files_in_folders(*folder_names):
    """
    Deletes all files in the specified folders within the current working directory.
    
    Parameters:
        folder_names (str): Names of the folders from which to delete all files.
    """
    # Construct the full path for each folder and delete files within
    for folder_name in folder_names:
        local_dir = os.path.join(os.environ["PWD"],folder_name)
        
        # Delete each file in the folder
        for file_name in os.listdir(local_dir):
            file_path = os.path.join(local_dir, file_name)
            os.remove(file_path)
            print(f"Deleted file: {file_path}")
    
    print("All files deleted from the specified folders.")


##################### Download heliumplus dumps from SFTP server ######################
def heliumplus_dumps_download():
            # Local directory to save downloaded files
            folder_name = "dumps-gz"
            local_dir = os.path.join(os.environ["PWD"], folder_name)

            # Get list of folders in sftp remote directory
            remote_dir = '/home/helium/heliumplus_weekly'
            extension = '.gz'
            folders = []

            for folder in sftp.listdir_attr(remote_dir):
                    folders.append("/home/helium/heliumplus_weekly/" + folder.filename)
                    folder_dirs = folders

            # # remove invalid folder       
            # folder_dirs.remove('/home/helium/heliumplus/datastore/FertilAid_Clinic')

            # Loop through each folder directory
            for folder in folder_dirs:
                # folder = folder + "/backups"
                print(folder)

                # Get list of files with extension gz in folder directory
                files = sftp.listdir(folder)
                dump_files = [f for f in files if f.endswith(extension)]
                print(dump_files)

                # print(local_dir)

                # remote_path = remote_dir
                # local_path = os.path.join(local_dir, os.path.basename(folder) + '.tar.gz')
                # sftp.get(remote_path, local_path)

                # Calculate the timestamp for 7 days ago from the current time
                current_time = datetime.datetime.now()
                seven_days_ago = current_time - datetime.timedelta(days=7)
                seven_days_ago_timestamp = seven_days_ago.timestamp()

                # Find the latest file
                latest_file = None
                latest_time = 0
                for file in dump_files:
                    remote_path = os.path.join(folder, file)
                    file_attr = sftp.stat(remote_path)
                    file_time = file_attr.st_mtime
                    # Check if the file's modification time is within the last 7 days
                    if file_time > seven_days_ago_timestamp and file_time > latest_time:
                    # if file_time > latest_time:
                        latest_file =  file
                        latest_time = file_time
                        # print(latest_time)

                # Download the latest file to local directory
                if latest_file:
                    # print(latest_file)
                    remote_path = os.path.join(folder, latest_file)
                    local_path = os.path.join(local_dir, os.path.basename(folder) + '.tar.gz')
                        

                    sftp.get(remote_path, local_path)



##################### Unzip dumps files ######################
def unzip_dumps():
            # specify the directory path
            folder_name = "dumps-gz/"
            folder_path = os.path.join(os.environ["PWD"], folder_name)

            # get a list of all folders name in the specified directory
            folder_names = os.listdir(folder_path)
                
            # safely remove '.DS_Store' if it exists
            if '.DS_Store' in folder_names:
                folder_names.remove('.DS_Store')

            for folder_name in folder_names:
                input_file = folder_path + folder_name
                output_file = input_file.replace(".tar.gz", ".sql").replace("-gz", "-sql")

            # open the input .gz file in binary mode
                with gzip.open(input_file, "rb") as f_in:
                    # open the output file in write mode
                    with open(output_file, "wb") as f_out:
                        # copy the contents of the .gz file to the output file
                        f_out.write(f_in.read())

                # create the new lowercase file name
                lowercase_output_file = output_file.lower()
        
                # rename the file to lowercase
                os.rename(output_file, output_file)
                # os.rename(output_file, lowercase_output_file)

            folder_name = "dumps-sql/"

            for filename in os.listdir(folder_name):
                old_path = os.path.join(folder_name, filename)
                new_path = os.path.join(folder_name, filename.lower())

                if old_path != new_path:  # Avoid errors if already lowercase
                    os.rename(old_path, new_path)
                    print(f"Renamed: {filename} -> {filename.lower()}")


# execute function
if __name__ == "__main__":
    delete_all_files_in_folders('dumps-gz','dumps-sql')  # Replace with your actual folder name
    heliumplus_dumps_download()
    unzip_dumps()


# Close SFTP connection
sftp.close()
transport.close()
