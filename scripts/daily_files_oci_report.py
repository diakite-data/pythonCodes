import paramiko
import subprocess
from datetime import datetime, timedelta
import argparse
import traceback

# construct the argument parser and parse the arguments
ap = argparse.ArgumentParser()
ap.add_argument("--date", required=True, help="date to enter format yyyyMMdd")
args = vars(ap.parse_args())

def get_zip_files_gl_launch_job(dt) : 

    try : 
            loadingdate = dt

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
            command = f'scp -oPort=2221 dwhuser@10.232.117.115:/opt/cft/v3.0.1/Transfer_CFT/runtime/pub/OMCLOUD/OCI_DWH_report_{loadingdate}.zip /home/glrep_user/OCI_DWH_report_{loadingdate}.zip && scp -i /home/glrep_user/Keys/sunshine-key.pem /home/glrep_user/OCI_DWH_report_{loadingdate}.zip sunshine@10.238.39.5:/home/sunshine/ && rm -rf /home/glrep_user/OCI_DWH_report_{loadingdate}.zip'
            stdin, stdout, stderr = client.exec_command(command)

            # Read the command output
            output = stdout.read().decode()

            # Print the output
            print(output)

            # Close the SSH connection
            client.close()

            print("*********************** Unzip and Launch Process ***********************")

            # Command to execute
            command = f''' unzip /home/sunshine/OCI_DWH_report_{loadingdate}.zip -d /nfs_sunshine/OCI_DWH_report_{loadingdate} && cp /nfs_sunshine/OCI_DWH_report_{loadingdate}/Transactions_{loadingdate}.csv /nfs_sunshine/ && cp /nfs_sunshine/OCI_DWH_report_{loadingdate}/AllUsers_{loadingdate}.csv /nfs_sunshine/ && cp /nfs_sunshine/OCI_DWH_report_{loadingdate}/Subscribers_{loadingdate}.csv /nfs_sunshine/ && sh /home/sunshine/script/copyOM_files_hdfs.sh ${loadingdate} && rm -rf /nfs_sunshine/OCI_DWH_report_{loadingdate} && rm -rf /nfs_sunshine/Transactions_{loadingdate}.csv ; rm -rf /nfs_sunshine/AllUsers_{loadingdate}.csv ; rm -rf /nfs_sunshine/Subscribers_{loadingdate}.csv && sh job_random_commission_fixe_cokpit.sh'''

            # Execute the command and capture the output
            output = subprocess.check_output(command, shell=True, universal_newlines=True)

            # Print the output
            print(output)

    except Exception as e: 
                        
            command = f''' curl "http://10.1.60.190:9802/dispatcher/httpconnectserver/smsaffaires.jsp?UserName=application_smsaffaires_sva&Password=sva&SenderAppId=1&DA=2250708459837&SOA=OCI_REPORT&Flags=264192&Content=Getting+files+not+ok+-+date+:+{dt}" '''

            # Execute the command and capture the output
            output = subprocess.check_output(command, shell=True, universal_newlines=True)

            # Print the output
            print(output)

            print("An error occurred:", e)
            traceback.print_exc()

if __name__ == '__main__':

    print(get_zip_files_gl_launch_job(args["date"]))

    


