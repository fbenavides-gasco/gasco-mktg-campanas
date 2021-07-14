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


## Normalización de nombres columnas:
def norm_col_name(df):
    replacements = (
        ("á", "a"),
        ("é", "e"),
        ("í", "i"),
        ("ó", "o"),
        ("ú", "u"),
    )
    col_names = df.columns.str.strip().str.lower().str.replace(' ', '_').str.replace('(', '').str.replace(')', '')

    for a,b in replacements:
        col_names = col_names.str.replace(a,b)
    df.columns = col_names
    return df

clientes_open = norm_col_name(clientes_open)

## Rango de fecha de evaluacion de la campaña
fecha_ini_campana = clientes_open['fecha_envio'][0].date()
fecha_ter_campana = clientes_open['fecha_ventana'][0].date()
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