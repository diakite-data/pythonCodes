#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
### Tutorial Documentation
Documentation that goes along with the Airflow tutorial located
[here](https://airflow.apache.org/tutorial.html)
"""
# [START tutorial]
# [START import_module]
from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
#from airflow.operators.email_operator import EmailOperator
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.ssh.hooks.ssh import SSHHook
from datetime import datetime, timedelta


# [END import_module]

# sshHook = SSHHook(ssh_conn_id="conn-id", key_file='/opt/airflow/keys/ssh.key')
# a hook can also be defined directly in the code:

#REMOTE HOSTS SERVERS : oy-edge-cl-01 / oy-master-cl-02 / oy-master-cl-01
sshHook = SSHHook(remote_host='oy-edge-cl-01', username='sunshine', key_file='/var/keys/rsa_sunshine_private.pem')


# [START instantiate_dag]
with DAG(
    'random_objectif_hebdomadaire',
    # [START default_args]
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'owner':'DRSI/Analytics',
        'email': ['youssouf.diakite@orange.com'],
        'email_on_failure': True,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        #'end_date': datetime(2022, 6, 6),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    # [END default_args]
    description="RANDOM OBJECTIF HEBDOMADAIRE" ,
    schedule_interval="00 11 1,8,15,22,29 * *",
    start_date=datetime(2022, 9, 20, 19, 30),
    catchup=False,
    tags=['sunshine', 'RANDOM OBJECTIF HEBDOMADAIRE'],
) as dag:
    # [END instantiate_dag]
    
        date = datetime.now() + timedelta(days=-1)        
        loadingdate = datetime.strftime(date , '%Y%m%d')
        taux_agent=0.04
        taux_grossiste=0.025

        start_task = EmptyOperator(
        task_id='start'
                )

        spark_submit_bpdv = SSHOperator(
            task_id="BASE PDV",
            command= f"spark-submit --conf spark.port.maxRetries=300 --conf spark.network.timeout=900 --class ci.orange.orangemoney.jobs.random.Base_pdv --jars /home/sunshine/jars/config-1.3.1.jar,/home/sunshine/jars/zkclient-0.9.jar,/home/sunshine/jars/elasticsearch-spark-20_2.11-6.5.4.jar,/home/sunshine/jars/javax.mail-1.6.2.jar --master yarn --queue random --driver-memory 12g /home/sunshine/jobs/oci_projects_2.11-0.ys.jar {loadingdate}",
            ssh_hook = sshHook)

        spark_submit_agent = SSHOperator(
            task_id="OBJECTIF AGENT",
            command= f"spark-submit --conf spark.port.maxRetries=300 --conf spark.network.timeout=900 --class ci.orange.orangemoney.jobs.random.Objectif_hebdomadaire_agent --jars /home/sunshine/jars/config-1.3.1.jar,/home/sunshine/jars/elasticsearch-spark-20_2.11-6.5.4.jar,/home/sunshine/jars/javax.mail-1.6.2.jar --master yarn --executor-cores 10 --num-executors 15 --executor-memory 20g --driver-cores 10 --driver-memory 10g --queue random --deploy-mode cluster /home/sunshine/jobs/oci_projects_2.11-0.ys.jar prod_dexa {loadingdate} 1 {taux_agent} {taux_grossiste}",
            ssh_hook = sshHook)

        spark_submit_grossiste = SSHOperator(
            task_id="OBJECTIF GROSSISTE",
            command= f"spark-submit --conf spark.port.maxRetries=300 --conf spark.network.timeout=900 --class ci.orange.orangemoney.jobs.random.Objectif_hebdomadaire_grossiste --jars /home/sunshine/jars/config-1.3.1.jar,/home/sunshine/jars/elasticsearch-spark-20_2.11-6.5.4.jar,/home/sunshine/jars/javax.mail-1.6.2.jar --master yarn --executor-cores 10 --num-executors 15 --executor-memory 20g --driver-cores 10 --driver-memory 10g --queue random --deploy-mode client /home/sunshine/jobs/oci_projects_2.11-0.ys.jar prod_dexa {loadingdate} 1 {taux_agent} {taux_grossiste}",
            ssh_hook = sshHook)

        end_task = EmptyOperator(
        task_id='end')

        start_task >> spark_submit_bpdv >>  spark_submit_agent  >> spark_submit_grossiste  >> end_task

# [END tutorial]
