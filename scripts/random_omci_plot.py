import paramiko
import subprocess
from datetime import datetime, timedelta
import argparse
import traceback
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import datetime
from datetime import datetime

ap = argparse.ArgumentParser()
ap.add_argument("-e", "--date", required=True,
                help="date to enter format yyyyMMdd")
args = vars(ap.parse_args())

try : 

    start_date = args["date"]
    start_date = datetime.strptime(start_date, '%Y%m%d')
    days_ago = start_date - timedelta(days=18)
    end_date = days_ago.strftime('%Y%m%d')

    command = f''' 
    start_date=$(date "+%Y%m%d" -d "18 days ago")
    end_date=$(date "+%Y%m%d")

    current_date="$start_date"

    while [[ "$current_date" != "$end_date" ]]; do
    echo "************** $current_date  **************"
    curl -XGET "http://10.242.71.159:9200/_cat/indices/random_commissions_fixes_v2_$current_date?h=index,docs.count,creation.date.string&v=" >> /home/sunshine/fichiers/random_omci_{args["date"]}.csv
    current_date=$(date "+%Y%m%d" -d "$current_date + 1 day")
    done

    echo "************** $end_date **************"
    '''

    output = subprocess.check_output(command, shell=True, universal_newlines=True)

    # Print the output
    print(output)

    df = pd.read_csv(f'/home/sunshine/fichiers/random_omci_{args["date"]}.csv', sep = " ")
    df1 = df[["index", "Unnamed: 6", "Unnamed: 7"]].rename(columns={ "index" : "index", "Unnamed: 6" : "count" , "Unnamed: 7" : "time" })
    df2 = df1[df1["index"] != "index"]
    df2.head()

    def get_day(char) : 
        return datetime.fromisoformat(char[:-1]).strftime("%Y-%m-%d")
        
    def get_hour(char) : 
        return datetime.fromisoformat(char[:-1]).strftime("%H")   

    def get_day_hour(char) : 
        return datetime.fromisoformat(char[:-1]).strftime("%Y-%m-%d %H")  

    def get_date_change_format(index) : 
        return datetime.strptime(index[-8:], "%Y%m%d").strftime("%Y-%m-%d")

    df2["day"] = df2["index"].astype(str).apply(get_date_change_format)
    df2["hour"] = df2["time"].astype(str).apply(get_hour)
    df2.head()
    df2 = df2.tail(14)

    x = list(df2["day"])
    y = list(df2["hour"])
    yy = [int(i) for i in y]

    plt.style.use('ggplot')
    plt.figure(figsize=(16, 4))
    plt.xticks(rotation=45, ha='right')
    plt.plot(x,yy, color='blue', marker='o', linestyle='dashed')
    plt.axis(ymin = 5, ymax = 23)
    plt.xlabel('Days', fontweight='bold')
    plt.ylabel('Hours', fontweight='bold')
    plt.title('RANDOM OMCI Time Index Insertion Series', fontsize=16, fontweight='bold', color='red') 
    plt.axhline(y=9, color='green', linestyle='--')
    plt.tight_layout()
    plt.xticks(fontweight='bold')
    plt.yticks(fontweight='bold')
    plt.savefig(f'/home/sunshine/fichiers/plot_romci_{args["date"]}.png')

    # SSH credentials and server information
    username = 'arkab'
    password = 'gWYmbeOM4ZX5Ld'
    hostname = '10.242.68.63'
    port = 22

    # Local file path
    local_file_path = f'/home/sunshine/fichiers/plot_romci_{args["date"]}.png'

    # Remote file path
    remote_file_path = f'/srv/tomcat/webapps/assets/img/plot_romci_{args["date"]}.png'

    # Connect to the remote server using SSH
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=hostname, port=port, username=username, password=password)

    # Open an SFTP session
    sftp = ssh.open_sftp()

    # Copy the local file to the remote server
    sftp.put(local_file_path, remote_file_path)

    sftp.close()

    command = f''' 
    rm -rf  /home/sunshine/fichiers/random_omci_{args["date"]}.csv
    rm -rf  /home/sunshine/fichiers/plot_romci_{args["date"]}.png
    '''

    output = subprocess.check_output(command, shell=True, universal_newlines=True)

    # Print the output
    print(output)

except Exception as e: 
                        
            command = f''' curl "http://10.1.60.190:9802/dispatcher/httpconnectserver/smsaffaires.jsp?UserName=application_smsaffaires_sva&Password=sva&SenderAppId=1&DA=2250708459837&SOA=RANDOM-G&Flags=264192&Content=Go+to+check+graph+random:+{args["date"]}" '''

            # Execute the command and capture the output
            output = subprocess.check_output(command, shell=True, universal_newlines=True)

            # Print the output
            print(output)

            print("An error occurred:", e)
            traceback.print_exc()

