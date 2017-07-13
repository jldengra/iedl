
# coding: utf-8

# In[4]:

## Importación al bucket iedatalakeprocess de datos de AX (tratados, unificados y normalizados)
## ---------------------------------------------------------------------------------------------

import pandas as pd
import csv
import s3fs
import numpy as np
import os
import datetime
import uuid


# In[5]:

## Proceso, versíón y fecha de ingesta para identificar la ejecución

transformationProcess = 'Transformacion de datos AX de landing a process'
transformationVersion = str(uuid.uuid1())
transformationInitialDate = str(datetime.datetime.now())


# In[37]:

# entityName = 'LEDGERTABLE', csvIndex = 'DATAAREAID, ACCOUNTNUM'
# # Atributos de interés de la entidad ledgertable

ledgertableAttributes = ['DATAAREAID', 'ACCOUNTNUM', 'ACCOUNTNAME', 'ACCOUNTPLTYPE']
ledgertable = pd.read_csv('s3://iedatalakelanding/ax/LEDGERTABLE.csv.gz', sep = ';', 
                         compression = 'gzip', quotechar='|', quoting=csv.QUOTE_MINIMAL, 
                         encoding = 'utf8', usecols = ledgertableAttributes )


# In[29]:

# Enumerado LedgerAccountType según especificación de Microsoft (a falta de saber dónde se traduce en base de datos)
# https://msdn.microsoft.com/es-es/library/aa595460(v=ax.50).aspx

ledgerAccountType = pd.read_csv('s3://iedatalakelanding/ax/Enumeration_LedgerAccountType.csv', sep = ';')


# In[30]:

ledgerAccountType


# In[31]:

ledgertable.head()


# In[42]:

#ledgertable  #[['ie_programid', 'value']] \
ledgertable = pd.merge(ledgertable, ledgerAccountType, left_on = 'ACCOUNTPLTYPE', 
        right_on = 'Value', how  ='left') \
        [['DATAAREAID', 'ACCOUNTNUM', 'ACCOUNTNAME', 'Description']].drop_duplicates()

ledgertable = ledgertable.rename(columns = {'Description':'LedgerAccountType'})

# Se requiere normalizar en mayúsculas el campo DATAAREAID según indicó Raúl Pleite
ledgertable['DATAAREAID']
    


# In[44]:

ledgertable['DATAAREAID'].upper()


# In[43]:

ledgertable


# In[ ]:




# In[ ]:




# In[12]:

# Enumerado LedgerAccountType según especificación de Microsoft (a falta de saber dónde se traduce en base de datos)
# https://msdn.microsoft.com/es-es/library/aa595460(v=ax.50).aspx

dfLedgerAccountType = pd.DataFrame(columns = ('Name', 'Value', 'Description'))


# In[20]:

dfLedgerAccountType.append(pd.Series({'Name': 'AccountOperations', 'Value': 0, 'Description': 'Profit and loss'}), 
                          ignore_index=True)
dfLedgerAccountType.append(pd.Series({'Name': 'AccountOperations', 'Value': 0, 'Description': 'Profit and loss'}), 
                          ignore_index=True)
dfLedgerAccountType.append(pd.Series({'Name': 'AccountOperations', 'Value': 0, 'Description': 'Profit and loss'}), 
                          ignore_index=True)
dfLedgerAccountType.append(pd.Series({'Name': 'AccountOperations', 'Value': 0, 'Description': 'Profit and loss'}), 
                          ignore_index=True)
dfLedgerAccountType.append(pd.Series({'Name': 'AccountOperations', 'Value': 0, 'Description': 'Profit and loss'}), 
                          ignore_index=True)
dfLedgerAccountType.append(pd.Series({'Name': 'AccountOperations', 'Value': 0, 'Description': 'Profit and loss'}), 
                          ignore_index=True)
dfLedgerAccountType.append(pd.Series({'Name': 'AccountOperations', 'Value': 0, 'Description': 'Profit and loss'}), 
                          ignore_index=True)
dfLedgerAccountType.append(pd.Series({'Name': 'AccountOperations', 'Value': 0, 'Description': 'Profit and loss'}), 
                          ignore_index=True)
dfLedgerAccountType.append(pd.Series({'Name': 'AccountOperations', 'Value': 0, 'Description': 'Profit and loss'}), 
                          ignore_index=True)


# In[21]:

dfLedgerAccountType.shape


# In[ ]:




# In[ ]:




# In[5]:

ledgertable.shape


# In[8]:

ledgertable.columns


# In[7]:

ledgertable


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:

# Atributos de interés de la entidad IE_Program

ie_program_attributes = ['ie_programid', 'ie_ieprogramid', 'ie_name', 'ie_longname', 'ie_shortname', 
                         'ie_programtypeid', 'ie_agrupationid', 'ie_studytype_displayname', 
                         'ie_format_displayname', 'ie_belongto_displayname', 
                         'ie_campusplace_displayname', 'ie_location_displayname', 'ie_languageidname',
                         'ie_secretaryidname', 'ie_dirigidoa', 'createdon', 'ie_directoridname', 
                         'ie_programtype', 'statecode_displayname', 'ie_commercialized', 
                         'ie_academiccatalog', 'ie_comercialcatalog', 'ie_untrueprogram']

# Lectura en un DataFrame del CSV donde se ha volcado la entidad IE_Program en bruto en la capa de 
# landing

ie_program = pd.read_csv('s3://iedatalakelanding/crm/IE_Program.csv.gz', sep = ';', 
                         compression = 'gzip', quotechar='|', quoting=csv.QUOTE_MINIMAL, 
                         usecols = ie_program_attributes)

# Renombramos 'ie_name' con la etiqueta del programa en CRM para evitar confusión con nombre oficial

ie_program = ie_program.rename(columns = {'ie_name':'ie_program_crm_label'})


# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[ ]:




# In[1]:

## Importación al bucket iedatalakeLanding de entidades de Microsoft Dynamics AX
## ------------------------------------------------------------------------------
##
## Las entidades necesarias se extraerán de la base de datos de AX. 

import sqlalchemy
import csv
import string
import pandas as pd
import os
import shutil
import datetime
import uuid
import sys

# Importación de opciones de configuración

if (os.name == 'nt'):
    sys.path.append(os.environ["USERPROFILE"].replace('\\', '/') + '/iedatalake/python/config')
else: 
    sys.path.append('/home/ubuntu/iedatalake/python/config')
import databaseconfig as dbc


# In[2]:

## Proceso, versíón y fecha de ingesta para identificar la ejecución

ingestionProcess = 'Ingesta de Entidades AX desde analytics01 en python'
ingestionVersion = str(uuid.uuid1())
ingestionInitialDate = str(datetime.datetime.now())

## Motor SQLAlchemy para la base de datos SQL Server de AX

axEngine = sqlalchemy.create_engine(dbc.ax['dialect'] + '+' + dbc.ax['driver'] + '://'       + dbc.ax['username'] + ':' + dbc.ax['password'] + '@'       + dbc.ax['host'] + '/' + dbc.ax['database'] + dbc.ax['parameters'])

## Carpeta auxiliar para depositar los CSVs generados antes de subir

if not os.path.exists("data"):
    os.makedirs("data")


# In[4]:

## Procedimiento de importación de una entidad de AX a un fichero CSV local

def ImportAXEntity(entityName, csvIndex, **optionalParameters): 
    
    # Ruta del fichero CSV, asumida en una subcarpeta "data" en la carpeta donde se ejecuta
    # y extensión .csv.gz porque se generará comprimido en formato GZIP
    
    csvFile = optionalParameters.get('csvFile', 'data/' + entityName + '.csv.gz')
        
    # Parámetro opcional con el número de registros del bloque a recuperar de forma iterativa
    
    chunkSize = optionalParameters.get('chunkSize', None)
    if (chunkSize == None):
        # Si no viene de entrada ningún número de registros, no se limita, poniendo como alcance
        # el número total de filas de la tabla de la entidad original
        axConnection = axEngine.connect()
        df = pd.io.sql.read_sql("SELECT COUNT(*) as recuento FROM " + entityName, axConnection)
        chunkSize = int(df["recuento"]) + 1
        axConnection.close()
    else: 
        chunkSize = int(chunkSize)

    # Convertimos csvIndex a lista, pues llega como una cadena en el parámetro 
    # con valores separados por coma de la forma 'DATAAREAID, DIMENSIONCODE, NUM' 
    # dejando el valor original en orderBy para la ordenación en la consulta fraccionada
    
    orderBy = csvIndex    
    csvIndex = csvIndex.replace(' ', '').split(',')    
    axConnection = axEngine.connect()
    rowsPending = True
    offset = 0
    while rowsPending:
        sqlSentence = "WITH Results_SQL AS (SELECT "        + "ROW_NUMBER() OVER "         + "(ORDER BY " + orderBy + ") as RowNum, * "         + "FROM " + entityName + ") "         + "SELECT * FROM Results_SQL WHERE  RowNum > " + str(offset) + " AND RowNum <=  "         + str(offset + chunkSize)        
        chunk = pd.io.sql.read_sql(sqlSentence, axConnection)

        print (str(offset)  + " < RowNum <= " + str(offset + chunkSize))

        # Borramos columna auxiliar RowNum usada para ordenar por el índice
        del chunk["RowNum"]
        chunk.set_index(csvIndex, inplace = True)        
        if (offset == 0):
            chunk.to_csv(csvFile, sep = ';', compression = 'gzip', quotechar='|', quoting=csv.QUOTE_MINIMAL, 
                     encoding = 'utf8')
        else:            
            chunk.to_csv(csvFile, sep = ';', compression = 'gzip', quotechar='|', quoting=csv.QUOTE_MINIMAL, 
                     encoding = 'utf8', mode= 'a', header = False)               
        offset += chunkSize
        if (len(chunk) < chunkSize):
            rowsPending = False            
        del chunk

    axConnection.close()  
    


# In[5]:

ImportAXEntity(entityName = 'LEDGERTABLE', csvIndex = 'DATAAREAID, ACCOUNTNUM')


# In[11]:

ImportAXEntity(entityName = 'DIMENSIONS', csvIndex = 'DATAAREAID, DIMENSIONCODE, NUM')


# In[6]:

ImportAXEntity(entityName = 'LEDGERTRANS', csvIndex = 'DATAAREAID, RECID', chunkSize = 300000)


# In[ ]:




# In[12]:

## Sincronización de carpeta local "data" con carpeta "ax" del bucket S3 iedatalakelanding
## Hace uso de AWSCLI (previamente instalado mediante la instrucción: sudo apt install awscli)

ingestionFinalDate = str(datetime.datetime.now())
os.system('aws s3 sync data s3://iedatalakelanding/ax --metadata ingestionprocess="' + ingestionProcess  
          + '",ingestionversion="' + ingestionVersion + '",ingestioninitialdate="' + ingestionInitialDate 
          + '",ingestionfinaldate="' + ingestionFinalDate + '",code="Ingest-AX-Entities"');


# In[ ]:




# In[16]:




# In[ ]:



