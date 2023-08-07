import paramiko
import subprocess
from datetime import datetime, timedelta
import argparse
import traceback


# construct the argument parser and parse the arguments
ap = argparse.ArgumentParser()
ap.add_argument("--date", required=True, help="date to enter format yyyyMMdd")
ap.add_argument("--heure", required=True, help="heure")
args = vars(ap.parse_args())

def get_zip_files_gl_push_to_remote_server(dt, hr) : 

    try : 

            hr = int(hr)
            hour = eval(str(hr)) - 1
            loadingdate = dt

            if eval(str(hour)) == -1 :
                hour = 23
                date = datetime.now() + timedelta(days=-1)
                loadingdate = datetime.strftime(date , '%Y%m%d')

            if len(str(hour)) == 1 : 
                hour = "0" + str(hour)

            # Create an SSH client
            client = paramiko.SSHClient()

            # Automatically add the server's host key (not recommended for production use)
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Connect to the remote server
            hostname = '10.242.69.96'
            port = 22
            username = 'glrep_user'
            password = 'glrep_user@123'
            client.connect(hostname,port,username,password)

            print("*********************** Getting files from GL ***********************")

            # Execute a bash command
            command = f'scp -oPort=2221 dwhuser@10.232.117.115:/opt/cft/v3.0.1/Transfer_CFT/runtime/pub/OMCLOUD/Current-Transactions_{loadingdate}_{hour}00.zip /home/glrep_user/Current-Transactions_{loadingdate}_{hour}00.zip && scp -i /home/glrep_user/Keys/sunshine-key.pem /home/glrep_user/Current-Transactions_{loadingdate}_{hour}00.zip sunshine@10.238.39.2:/home/sunshine/transactions_horaires && rm -rf /home/glrep_user/Current-Transactions_{loadingdate}_{hour}00.zip'
            stdin, stdout, stderr = client.exec_command(command)

            # Read the command output
            output = stdout.read().decode()

            # Print the output
            print(output)

            # Close the SSH connection
            client.close()

            # Command to execute
            command = f''' unzip /home/sunshine/transactions_horaires/Current-Transactions_{loadingdate}_{hour}00.zip -d /fs_sunshine/Current-Transactions_{loadingdate}_{hour}00 && cut -d '|' -f 9,10,11 --complement /fs_sunshine/Current-Transactions_{loadingdate}_{hour}00/Current-Transactions_{loadingdate}_{hour}00.csv > /fs_sunshine/Current-Transactions_{loadingdate}_{hour}00/Current-Transactionss_{loadingdate}_{hour}00.csv '''

            # Execute the command and capture the output
            output = subprocess.check_output(command, shell=True, universal_newlines=True)

            # Print the output
            print(output)

            hostname = '10.242.38.23'
            port = 22
            username = 'ftpuser'
            password = 'Ftpu$er1!'

            # Create an SSH client
            client = paramiko.SSHClient()

            # Add the remote server's host key
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

            # Connect to the remote server
            client.connect(hostname, port, username, password)

            # Open an SFTP session
            sftp = client.open_sftp()

            local_file = f'/fs_sunshine/Current-Transactions_{loadingdate}_{hour}00/Current-Transactionss_{loadingdate}_{hour}00.csv'
            remote_file = f'/ftp-live/ftpdata/OM_Current_Transactions/Current-Transactions_{loadingdate}_{hour}00.csv'

            print("*********************** Pushing files to FLYTEXT SERVER ***********************")

            # Copy the file to the remote server
            sftp.put(local_file, remote_file)

            # Close the SFTP session
            sftp.close()

            # Close the SSH connection
            client.close()

            # remove directory
            command = f''' rm -rf /fs_sunshine/Current-Transactions_{loadingdate}_{hour}00 '''

            # Execute the command and capture the output
            output = subprocess.check_output(command, shell=True, universal_newlines=True)

            # Print the output
            print(output)

    except Exception as e: 
            
            hr = int(hr)
            hour = eval(str(hr)) - 1
            loadingdate = dt

            if eval(str(hour)) == -1 :
                hour = 23
                date = datetime.now() + timedelta(days=-1)
                loadingdate = datetime.strftime(date , '%Y%m%d')

            if len(str(hour)) == 1 : 
                hour = "0" + str(hour)
            
            command = f''' curl "http://10.1.60.190:9802/dispatcher/httpconnectserver/smsaffaires.jsp?UserName=application_smsaffaires_sva&Password=sva&SenderAppId=1&DA=2250708459837&SOA=FLYTEXT&Flags=264192&Content=Putting+files+notok+-+date+:+{loadingdate}:+{hour}" '''

            # Execute the command and capture the output
            output = subprocess.check_output(command, shell=True, universal_newlines=True)

            # Print the output
            print(output)

            print("An error occurred:", e)
            traceback.print_exc()

if __name__ == '__main__':

    print(get_zip_files_gl_push_to_remote_server(args["date"],args["heure"]))

    
