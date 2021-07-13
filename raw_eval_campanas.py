# -*- coding: utf-8 -*-
"""
Created on Tue Jul 13 11:26:53 2021

@author: fbenavides
"""

import pandas as pd

#Conexion a GCP - BigQuery
import pydata_google_auth
from google.cloud import bigquery
SCOPES = [
    'https://www.googleapis.com/auth/cloud-platform',
    'https://www.googleapis.com/auth/drive',
]
credentials = pydata_google_auth.get_user_credentials(
    SCOPES,
    auth_local_webserver=True,
)
client = bigquery.Client(project='gasco-analytics', credentials=credentials)


###############################################################################

## lectura del dataframe.
clientes_open = pd.read_excel('evaluaciones/Clima_29_06_2021.xlsx')
clientes_open.head(2)

## Seleccionamos las columnas importantes.
clientes_open = clientes_open[['Email Address', 'Phone Number','Fecha envío', 'Fecha ventana','Opened','Clicked']]
clientes_open.head(2)

## Rango de fecha de evaluacion de la campaña
fecha_ini_campana = clientes_open['Fecha envío'][0].date()
fecha_ter_campana = clientes_open['Fecha ventana'][0].date()
print(fecha_ini_campana,fecha_ter_campana)

###############################################################################

## Configuración del Job con parametros de fecha para query.
job_config = bigquery.QueryJobConfig(
query_parameters=[
    bigquery.ScalarQueryParameter('INI_DATE', 'DATE', fecha_ini_campana),
    bigquery.ScalarQueryParameter('TER_DATE', 'DATE', fecha_ter_campana),
    ]
)


#Leemos la query y creamos lista para guardar los periodos
with open('querys\query_eval_campana.txt') as f:
    query_eval = f.read()


## Ejecutamos la query y convertimos el resultado en un pandas dataframe.
query_job = client.query(query_eval, job_config = job_config)  # API request
df_iter = query_job.to_dataframe()