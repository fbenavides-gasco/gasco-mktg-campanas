# -*- coding: utf-8 -*-
"""
Created on Tue Jul 13 11:26:53 2021

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

def run_query_eval(fecha_ini_campana, fecha_ter_campana,client):
    from google.cloud import bigquery

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
    
    return query_job.to_dataframe()

def info_conversion(df_clientes, data_eval, tipo):
    emails = df_clientes[df_clientes[tipo] == 1].email_address.to_list()

    return data_eval[data_eval.email.isin(emails)]


## Lectura del dataframe.
clientes_open = pd.read_excel('evaluaciones/Clima_29_06_2021.xlsx',sheet_name=0)

## Conexion a bigquery
client = GCP_conn()

## Normalizamos el nombre de las columnas
clientes_open = norm_col_name(clientes_open)

## Seleccionamos las columnas importantes.
clientes_open = clientes_open[['email_address', 'phone_number','fecha_envio', 
                               'fecha_ventana','opened','clicked']]
clientes_open.dropna(inplace = True)

## Rango de fecha de evaluacion de la campaña
fecha_ini_campana = clientes_open['fecha_envio'][0].date()
fecha_ter_campana = clientes_open['fecha_ventana'][0].date()
print(fecha_ini_campana,fecha_ter_campana)

## Generamos la data de evaluación de la campaña
data_eval = run_query_eval(fecha_ini_campana, fecha_ter_campana, client)

## Creación DF por clientes abiertos - click
df_open  = info_conversion(clientes_open, data_eval,'opened')
df_click = info_conversion(clientes_open, data_eval,'clicked')
print(len(df_open),len(df_click))

## Guardamos df_open y df_click en directorio correspondiente

#df_open.to_csv('compras_clientes_open_RECETA_20210712.csv', sep = ";")
#df_click.to_csv('compras_clientes_click_RECETA_20210712.csv', sep = ";")

###############################################################################

###############################################################################
#
#               2° Parte: Comparacion GC/GO
#
###############################################################################

## Leemos dataframes desde carpetas grupos
df_obj     = pd.read_csv('grupos\experimento_clima_20210629_obj.csv')#,index=False)
df_control = pd.read_csv('grupos\experimento_clima_20210629_control.csv') #,index=False)


####################
##  2.1 Clientes  ##
####################

email_obj    = df_obj.email.unique()
email_control = df_control.email.unique()

email_eval = data_eval.email.unique()

obj_compra     = list(set(email_obj) & set(email_eval))
control_compra = list(set(email_control) & set(email_eval))

print(100*len(obj_compra)/len(email_obj))
print(100*len(control_compra)/len(email_control))
##############################################################################

###################
##  2.1 Pedidos  ##
###################

#################
##  2.1 Gasto  ##
#################

#####################
##  2.1 Cilindros  ##
#####################
