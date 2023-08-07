 
import paramiko
import os
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import argparse

ap = argparse.ArgumentParser()
ap.add_argument("-e", "--date", required=True,
                help="date to enter format yyyyMMdd")
args = vars(ap.parse_args())

matplotlib.style.use('ggplot')

df = pd.read_csv(f'/nfs_sunshine/saving_data_mood/Mood_Etancheite_DG_{args["date"]}.csv', sep = ";")
df.head()

df.dropna(inplace = True)

df["difference_valeur"] = df["difference_valeur1"] + df["difference_valeur2"]
df["difference_volume"] = df["difference_volume1"] + df["difference_volume2"]

df["valeur_gr"] = df["valeur_gr1"] + df["valeur_gr2"]
df["volume_gr"] = df["volume_gr1"] + df["volume_gr2"]

df["valeur_mood"] = df["valeur_mood1"] + df["valeur_mood2"]
df["volume_mood"] = df["volume_mood1"] + df["volume_mood2"]

df3 = df.groupby("partenaire_new", as_index=False).agg( difference_valeur = ("difference_valeur","sum") ,
                                  difference_volume = ("difference_volume", "sum") ,  
                                  valeur_gr = ("valeur_gr", "sum") ,
                                  volume_gr = ("volume_gr","sum"),  
                                  valeur_mood = ("valeur_mood", "sum"), 
                                  volume_mood = ("volume_mood", "sum") 
                                )

df3["difference_valeur"] = df3["difference_valeur"].apply(lambda x : x if x > 0 else 0)
df3["difference_volume"] = df3["difference_volume"].apply(lambda x : x if x > 0 else 0)

print(" ............................. VALEUR .............................")

group1 = list(df3["valeur_gr"])
group2 = list(df3["valeur_mood"])
group3 = list(df3["difference_valeur"])

fig, ax = plt.subplots(figsize=(30,10))

bar_width = 0.2

bar1 = ax.bar(x=range(len(group1)), height=group1, width=bar_width, label='valeur GR', color='purple')
bar2 = ax.bar(x=[i + bar_width for i in range(len(group2))], height=group2, width=bar_width, label='valeur MOOD', color='red')

ax2 = ax.twinx()
bar3 = ax2.bar(x=[i + 2 * bar_width for i in range(len(group3))], height=group3, width=bar_width, label='Difference', color='black')

ax.set_xticks([i + bar_width for i in range(len(group1))])
ax.set_xticklabels(list(df3["partenaire_new"]))

# set y-axis limits for Group 1 and Group 2
ax.set_ylim(0, 3000000000)

# set y-axis step size for Group 1 and Group 2
ax.set_yticks(range(0, 3000000000, 500000000))
ax.ticklabel_format(style='plain', axis='y')

# set y-axis limits for Group 3
ax2.set_ylim(0, max(group3))

# set y-axis step size for Group 3
ax2.set_yticks(range(0, int(max(group3)) + 10000, int(max(group3)/10)))
ax2.ticklabel_format(style='plain', axis='y')

# Add values on the plot (only for Group 3)
x = [i + 2 * bar_width for i in range(len(group3))]
for i, v in enumerate(group3):
    ax2.text(x[i]+bar_width, v-10 ,  str(v*10**(-6))[0:4] + "M", color='black', fontweight='bold', fontsize=20)

ax.legend(loc='upper left')
ax2.legend(loc='upper right')

plt.title(f'Valeur {args["date"]}', fontsize = "20",fontstyle="italic", fontweight="bold")
plt.savefig(f'/nfs_sunshine/saving_data_mood/Valeur_{args["date"]}_mood.png')

print(" ............................. VOLUME .............................")

group1 = list(df3["volume_gr"])
group2 = list(df3["volume_mood"])
group3 = list(df3["difference_volume"])

df3["difference_volume"] = df3["difference_volume"].apply(lambda x : x if x > 0 else 0)

fig, ax = plt.subplots(figsize=(30,10))

bar_width = 0.2

bar1 = ax.bar(x=range(len(group1)), height=group1, width=bar_width, label='volume GR', color='purple')
bar2 = ax.bar(x=[i + bar_width for i in range(len(group2))], height=group2, width=bar_width, label='volume MOOD', color='red')

ax2 = ax.twinx()
bar3 = ax2.bar(x=[i + 2 * bar_width for i in range(len(group3))], height=group3, width=bar_width, label='Difference', color='black')

ax.set_xticks([i + bar_width for i in range(len(group1))])
ax.set_xticklabels(list(df3["partenaire_new"]))

# set y-axis limits for Group 1 and Group 2
ax.set_ylim(0, 100000)

# set y-axis step size for Group 1 and Group 2
ax.set_yticks(range(0, 100000, 20000))

# set y-axis limits for Group 3
ax2.set_ylim(0, max(group3))

# set y-axis step size for Group 3
ax2.set_yticks(range(0, int(max(group3)) + 100, int(max(group3)/10)))


# Add values on the plot (only for Group 3)
x = [i + 2 * bar_width for i in range(len(group3))]
for i, v in enumerate(group3):
    ax2.text(x[i]+bar_width, v , str(v), color='black', fontweight='bold', fontsize=20)

ax.legend(loc='upper left')
ax2.legend(loc='upper right')

plt.title(f'Volume {args["date"]}', fontsize = "20", fontstyle="italic", fontweight="bold")
plt.savefig(f'/nfs_sunshine/saving_data_mood/Volume_{args["date"]}_mood.png')

print(" ............................. SEND FILES .............................")

# SSH credentials and server information
username = 'arkab'
password = 'gWYmbeOM4ZX5Ld'
hostname = '10.242.68.63'
port = 22

# Local file path
local_file_path = f'/nfs_sunshine/saving_data_mood/Volume_{args["date"]}_mood.png'

# Remote file path
remote_file_path = f'/srv/tomcat/webapps/assets/img/Volume_{args["date"]}_mood.png'

# Connect to the remote server using SSH
ssh = paramiko.SSHClient()
ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
ssh.connect(hostname=hostname, port=port, username=username, password=password)

# Open an SFTP session
sftp = ssh.open_sftp()

# Copy the local file to the remote server
sftp.put(local_file_path, remote_file_path)

# Local file path
local_file_path = f'/nfs_sunshine/saving_data_mood/Valeur_{args["date"]}_mood.png'

# Remote file path
remote_file_path = f'/srv/tomcat/webapps/assets/img/Valeur_{args["date"]}_mood.png'

# Copy the local file to the remote server
sftp.put(local_file_path, remote_file_path)

# Close the SFTP session and SSH connection
sftp.close()
ssh.close()

