# autoload
import os, sys
working_dir = os.getcwd()
sys.path.append(working_dir) if working_dir not in sys.path else None

# import
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# define dags
model_sales_dag = DAG(
  dag_id="model_sales",
  default_args={
    "owner": "datvq",
    "catchup": False,
  },
  schedule="0 7,9,11,13,15,17 * * *",
  start_date=datetime(2025, 1, 1),
)

# define tasks
def model_sales_import():
  import lib.dx as dx
  import app.model.model_sales as model_sales
  dx.reload(model_sales)
  model_sales.execute(output_dir=model_sales.output_dir_default())

# define flows
model_sales_flow = PythonOperator(task_id="model_sales", python_callable=model_sales_import, dag=model_sales_dag)

# control flow
model_sales_flow
