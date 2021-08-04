# -*- coding: utf-8 -*-
"""
Created on Thu Jul 15 17:52:32 2021

@author: fbenavides
"""

###############################################################################
#
#               Funciones
#
###############################################################################

import pandas as pd

## Conexion al proyecto de bigquery
def GCP_conn(project:str  = 'gasco-analytics'):
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

def run_query_audiencia_clima(client,
                              dia_referencia:pd.datetime,
                              dias_forecast:int = 2,
                              min_recencia:int  = 15,
                              max_recencia:int  = 430) -> pd.DataFrame:
    from google.cloud import bigquery

    ## Configuración del Job con parametros de fecha para query.
    job_config = bigquery.QueryJobConfig(
    query_parameters=[
        bigquery.ScalarQueryParameter('DIA_REF', 'DATE', dia_referencia),
        bigquery.ScalarQueryParameter('DIAS_FORECAST', 'INT64', dias_forecast),
        bigquery.ScalarQueryParameter('MIN_RECENCIA', 'INT64', min_recencia),
        bigquery.ScalarQueryParameter('MAX_RECENCIA', 'INT64', max_recencia)
        ]
    )

    #Leemos la query y creamos lista para guardar los periodos
    with open('querys\query_audiencia_clima_parametrizada.txt') as f:
        query_eval = f.read()


    ## Ejecutamos la query y convertimos el resultado en un pandas dataframe.
    query_job = client.query(query_eval, job_config = job_config)  # API request
    
    return query_job.to_dataframe()

#Funcion para eliminar las tildes de las palabras
def normalize(s:str) -> str:
    replacements = (
        ("á", "a"),
        ("é", "e"),
        ("í", "i"),
        ("ó", "o"),
        ("ú", "u"),
    )
    for a, b in replacements:
        s = s.replace(a, b).replace(a.upper(), b.upper())
    return(s.upper())

##############################################################################

###############################################################################
#
#               Ejecucion Script
#
###############################################################################

##########################
##  Extraer bbdd clima  ##
##########################

from datetime import date
import numpy as np

## Conexion a bigquery
client = GCP_conn()

dia_referencia = date.today()
dias_forecast  = 2
min_recencia   = 15
max_recencia   = 430 

df_clima = run_query_audiencia_clima(client,dia_referencia,dias_forecast,
                                     min_recencia,max_recencia)

######################################
##  Aplicar criterios t° min y max  ##
######################################


def criterio_clima(df_clima:pd.DataFrame) -> pd.DataFrame:
    import pandas as pd
    import numpy  as np

    criterios_region = pd.read_excel('criterios_clima_regiones.xlsx',sheet_name = 'Regiones')
    criterios_RM     = pd.read_excel('criterios_clima_regiones.xlsx',sheet_name = 'RM')
    criterios_clima =  pd.concat([criterios_region,criterios_RM])

    criterios_clima['Comuna'] = criterios_clima.Comuna.apply(normalize)

    df_clima_2 = df_clima.merge(criterios_clima, how = 'left',
                          left_on = 'comuna', right_on = 'Comuna')
    df_clima_2['Temperatura Mínima'] = df_clima_2['Temperatura Mínima'].fillna(7)
    df_clima_2['temperatura Máxima'] = df_clima_2['temperatura Máxima'].fillna(15)
    
    df_clima_2['Región'] = df_clima_2['Región'].fillna('RM')
    df_clima_2 = df_clima_2[df_clima_2['Región'].isin(['RM', 'VIII', 'I', 'V'])]
    
    df_clima_2['Temperatura Mínima'] = pd.to_numeric(df_clima_2['Temperatura Mínima'], errors='coerce')
    df_clima_2['temperatura Máxima'] = pd.to_numeric(df_clima_2['temperatura Máxima'], errors='coerce')

    cond_1 = df_clima_2['apparentTemperatureMax'] < df_clima_2['temperatura Máxima']
    cond_2 = df_clima_2['apparentTemperatureMin'] < df_clima_2['Temperatura Mínima']

    df_clima_2['flag_temp_2'] = np.where(cond_1 & cond_2,1, 0)

    df_clima_2['criterio'] = df_clima_2['flag_rain'] + df_clima_2['flag_temp_2']
    
    return( df_clima_2[df_clima_2['criterio'] > 0] )

base_envio = criterio_clima(df_clima)

#################################################################
##  eliminar emails genericos/malos y comunas con solo 1 email ##
#################################################################

def limpiar_emails_comunas(df_clima:pd.DataFrame) -> pd.DataFrame:
    emails_genericos  = ['noresponder@gasconnect.cl', 'default@default.com']
    
    df_clima = df_clima[~df_clima['email'].isin(emails_genericos)]
    
    a = df_clima.groupby('comuna').count().reset_index()
    comunas_descarte = list(a[a['phone'] == 1].comuna.unique())
    print(comunas_descarte)
    
    df_clima = df_clima[~df_clima['comuna'].isin(comunas_descarte)]
    df_clima['apparentTemperatureMin'] = df_clima['apparentTemperatureMin'].round().apply(int)

    return(df_clima)

base_envio = limpiar_emails_comunas(base_envio)

##################################
##  Generación de bases (GO/GC) ##
##################################

columnas_x = ['phone', 'email', 'recency', 'predict_calefont',
              'predict_cocina', 'predict_estufa', 'predict_parrilla', 'forecast_date',
              'forecasted_date','apparentTemperatureMin', 'flag_temp_2', 'flag_rain']
columnas_y = ['comuna']

from sklearn.model_selection import train_test_split

clima_obj, clima_control, y_obj, y_control = train_test_split(base_envio[columnas_x], 
                                                              base_envio[columnas_y], 
                                                              test_size    = 0.30, 
                                                              random_state = 42, 
                                                              stratify     = base_envio[columnas_y])

df_obj = pd.concat([clima_obj,y_obj], axis = 1)
#df_obj = df_obj[['region_id','comuna_id','telefono','email']]

df_control = pd.concat([clima_control,y_control], axis = 1)
#df_control = df_control[['region_id','comuna_id','telefono','email']]

#df_obj.to_csv('experimento_clima_20210719_obj.csv',index=False)
#df_control.to_csv('experimento_clima_20210719_control.csv',index=False)