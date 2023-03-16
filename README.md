# gcp-dataproc_serverless-running-notebooks

# Objective
Orchestrator to run Notebooks on Dataproc Serverless via Cloud Composer

# File Directory Structure

**dataproc_serverless**
├── composer_input                   
│   ├── jobs/                       wrapper_papermill.py
│   ├── DAGs/                       serverless_airflow.py
├── notebooks 
│   ├── datasets/                   electric_vehicle_population.csv
│   ├── jupyter/                    spark_notebook.ipynb
│   ├── jupyter/output 

## File Details    
### composer_input
* **wrapper_papermill.py**: runs a papermill execution of input notebook and writes the output file into the assgined location
* **serverless_airflow.py**: orchestrates the workflow 

### notebooks 
* **spark_notebook.ibynp** : This file creates Spark Session for Electric Vehicle Population, loads csv file in GCS and perform fundamental Spark functions
* jupyter/output: this is where papermill notebook execution outputs will be stored

* electric_vehicle_population.csv: this [dataset](https://catalog.data.gov/dataset/electric-vehicle-population-data) shows the Battery Electric Vehicles (BEVs) and Plug-in Hybrid Electric Vehicles (PHEVs) that are currently registered through Washington State Department of Licensing (DOL).

# I) Orchestrating End to End Notebook Execution workflow in Cloud Composer 

1. Make sure to add [Variables](https://airflow.apache.org/docs/apache-airflow/stable/howto/variable.html) from Airflow UI for 

    - 'project_id': Variables.get('gcp_project'),
    - 'gcs_bucket': Variables.get('gcs_bucket'),
    - 'phs_region': Variables.get('phs_region'),
    - 'phs': Variables.get('phs'),
    

2. Create a Cloud Composer Environment

3. Find DAGs folder from Composer Environment and add composer_pyspark_notebook.py (DAGs file) to it in order to trigger DAGs execution:

DAG folder from Cloud Composer Console 
![Screenshot 2023-02-07 at 9 04 12 AM](https://user-images.githubusercontent.com/123537947/217266654-f7a017fb-7470-4e04-9803-a72be6f652bd.png)

4. Have all the files available in GCS bucket, except DAGs file which should go into your Composer DAGs folder

5. From Composer environment, create two Airflow variables (see composer_pyspark_notebook.py for the usage)
<img width="425" alt="Screenshot 2023-02-07 at 11 29 09 AM" src="https://user-images.githubusercontent.com/123537947/217304416-2522c177-a3eb-420c-bcfa-7dc4e571d820.png">

https://airflow.apache.org/concepts.html#variables
* gcp_project - Google Cloud Project to use for the Cloud Dataproc cluster.
* gce_zone - Google Compute Engine zone where Cloud Dataproc cluster should be
  created.

e. Open Airflow UI to monitor DAG executions, Runs and logs
    
![image](https://user-images.githubusercontent.com/123537947/215648916-811a8331-b61a-45a5-8f5a-b61f3fd4fdd0.png)

## II) Running a Jupyter notebook on a Dataproc cluster
(Notes: utilize [initialization script](composer_input/initialization_scripts) from this repository for python packages installation and GCS bucket mount)

Refer to this [GCP tutorial](https://cloud.google.com/dataproc/docs/tutorials/jupyter-notebook) to 
* Install the Dataproc Jupyter component on a new cluster 
* Connect to the Jupyter notebook UI running on the cluster from your local browser using the [Dataproc Component Gateway](https://cloud.google.com/dataproc/docs/concepts/accessing/dataproc-gateways)


## Closing Note
If you're adapting this example for your own use consider the following:

* Setting an appropriate input path within your environment (gcs, mounting point for gcsfuse, DAGs folder, etc)
* Setting more appropriate configurations (DAGs, Dataproc cluster, init_script for additional python packages, etc)
