import paramiko
import argparse


# Set up the connection details
hostname = '10.242.38.23'
port = 22
username = 'ftpuser'
password = 'Ftpu$er1!'

# construct the argument parser and parse the arguments
ap = argparse.ArgumentParser()
ap.add_argument("-e", "--date", required=True,
                help="date to enter format yyyyMMdd")
args = vars(ap.parse_args())

def put_files(local_file, remote_file) : 
    # Create an SSH client
    client = paramiko.SSHClient()

    # Add the remote server's host key
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    # Connect to the remote server
    client.connect(hostname, port, username, password)

    # Open an SFTP session
    sftp = client.open_sftp()

    # Copy the file to the remote server
    sftp.put(local_file, remote_file)

    # Close the SFTP session
    sftp.close()

    # Close the SSH connection
    client.close()


if __name__ == '__main__':

    local_file = f'/nfs_sunshine/FICHIER_TRANSACTIONSS_{args["date"]}.csv'
    remote_file = f'/ftp-live/ftpdata/OM_Current_Transactions/FICHIER_TRANSACTIONSS_{args["date"]}.csv'

    print(put_files(local_file, remote_file))

    local_file = f'/nfs_sunshine/FICHIER_SUBSCRIBERS_{args["date"]}.csv'
    remote_file = f'/ftp-live/ftpdata/OM_Subscribers/FICHIER_SUBSCRIBERS_{args["date"]}.csv'

    print(put_files(local_file, remote_file))






