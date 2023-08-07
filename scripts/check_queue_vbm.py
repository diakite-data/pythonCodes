import pandas as pd
import subprocess
import traceback

df_list = pd.read_csv("/home/sunshine/files/vbm_jobs.csv", sep = ";")
check_list = list(df_list["class"]) + list(df_list["app"])

command = """ yarn application --list | grep vbm | awk '{print $1,$2}' > /home/sunshine/files/vbm_jobs_running.csv """
output = subprocess.check_output(command, shell=True, universal_newlines=True)
print(output)
df_running = pd.read_csv("/home/sunshine/files/vbm_jobs_running.csv", sep = " ", names = ["app", "name"])
list_name = []
for i in list(df_running["name"]) : 
    if "." in i : 
        list_name.append(i.split(".")[-1])
    else :
        list_name.append(i)
print(f'List job running : {list_name}')
dictionary = dict(zip(list_name, list(df_running["app"])))
print(f'Dictionary of running job : {dictionary}')

for element in dictionary.keys() : 
    if element not in check_list :
        print(f'killing : {(element, dictionary[element])}')
        cmd = f'yarn application -kill {dictionary[element]}'
        output = subprocess.check_output(cmd, shell=True, universal_newlines=True)
        print(output)


