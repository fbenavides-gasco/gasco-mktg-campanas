# -*- coding: utf-8 -*-
"""
Created on Thu Jul 15 17:52:32 2021

@author: fbenavides
"""

###############################################################################
#
#               1° Parte: Evaluación clientes click/open
#
###############################################################################

import pandas as pd

## Conexion al proyecto de bigquery
def GCP_conn(project  = 'gasco-analytics'):
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
    client = bigquery.Client(project= project, credentials=credentials)
    
    return(client)

def run_query_audiencia_clima(dia_referencia,dias_forecast,
                              min_recencia,max_recencia,
                              client):
    from google.cloud import bigquery

    ## Configuración del Job con parametros de fecha para query.
    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter('DIA_REF', 'DATE', dia_referencia),
        bigquery.ScalarQueryParameter('DIAS_FORECAST', 'DATE', dias_forecast),
        bigquery.ScalarQueryParameter('MIN_RECENCIA', 'INT64', min_recencia),
        bigquery.ScalarQueryParameter('MAX_RECENCIA', 'INT64', max_recencia)
        ]
    )

    #Leemos la query y creamos lista para guardar los periodos
    with open('querys\query_audiencia_clima.txt') as f:
        query_eval = f.read()


    ## Ejecutamos la query y convertimos el resultado en un pandas dataframe.
    query_job = client.query(query_eval, job_config = job_config)  # API request
    
    return query_job.to_dataframe()
