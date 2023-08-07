import paramiko
import os



# connect to the Windows remote server
hostname = "10.242.66.21"
port = 22
transport = paramiko.Transport((hostname, port))

# provide the authentication credentials
transport.connect(username='admqv', password='FN7B%qy6@Kz2w9*')

# create an SFTP client object
sftp = paramiko.SFTPClient.from_transport(transport)



# specify the local file path
local_file = "/nfs_sunshine/FICHIER_ANNULATION_NEW_NEW_20230118.csv"

# specify the file path on the Windows remote server
remote_file = "C:\DATA\RANDOM_OMCI_DATAS\FICHIER_20230118.csv"

# upload the file
sftp.put(local_file, remote_file)

# close the SFTP client
sftp.close()

# close the transport connection
transport.close()

