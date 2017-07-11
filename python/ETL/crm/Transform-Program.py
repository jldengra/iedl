
# coding: utf-8

# <table width='100%'>
# <tr>
# <td>
# <center><h1 style="font-size:2em;color:#505050">Getting Data in Shape</h1>
# <h1 style="font-size:1em;color:#505050">Data Preprocesing - Data Munging - Data Wrangling</h1>
# </center>
# </td>
# 
# <td>
# <img align="middle" style="width:360px;" src = 'http://www.interlinkdata.co.uk/Images/CRMhead.png'>
# </td>
# </tr>
# </table>
# 
# 

# In[2]:

## Importación al bucket iedatalakeprocess de datos de CRM (tratados, unificados y normalizados)
## ---------------------------------------------------------------------------------------------
## Modelado de Programas

import pandas as pd
import csv
import s3fs
import numpy as np
import os
import datetime
import uuid


# In[3]:

## Proceso, versíón y fecha de ingesta para identificar la ejecución

transformationProcess = 'Transformacion de datos CRM de landing a process'
transformationVersion = str(uuid.uuid1())
transformationInitialDate = str(datetime.datetime.now())


# In[4]:

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


# In[5]:

# Atribución del literal 'N/A' para valores no disponibles en varias columnas

columnsLiteralNA = ['ie_studytype_displayname', 'ie_format_displayname', 'ie_belongto_displayname', 
                'ie_campusplace_displayname', 'ie_location_displayname', 'ie_languageidname',
                'ie_secretaryidname', 'ie_dirigidoa', 'ie_directoridname']

ie_program[columnsLiteralNA] = ie_program[columnsLiteralNA].fillna('N/A') 


# In[6]:

# Omitimos espacios a los lados en campos de texto donde pudieran estar de más para normalizarlos

columnsForTrim = ['ie_dirigidoa', 'ie_location_displayname', 'ie_program_crm_label', 'ie_directoridname', 
                 'ie_secretaryidname']

ie_program[columnsForTrim] = ie_program[columnsForTrim].applymap(str.strip)


# In[7]:

# Atribución de nombre corto al nombre largo de programas en que no viene informado

ie_program['ie_longname'] = ie_program['ie_longname'].fillna(ie_program['ie_shortname'])

# Si persiste algún nulo en el nombre largo es porque se da la anomalía de que no tiene ni nombre
# corto ni nombre largo, y tomamos como nombre largo la etiqueta del programa en CRM, y lo mismo
# para el nombre corto

ie_program['ie_longname']  = ie_program['ie_longname'].fillna(ie_program['ie_program_crm_label'])
ie_program['ie_shortname'] = ie_program['ie_shortname'].fillna(ie_program['ie_program_crm_label'])

# Normalizamos los valores de integración con el sistema académico dejando a NaN los no válidos
# por ser cero o mayores que un millón. El tipo de dato es float para admitir NaN

filterInvalidCorporateId = (ie_program['ie_ieprogramid'] == 0)                            | (ie_program['ie_ieprogramid'] > 1000000)
ie_program.loc[filterInvalidCorporateId, 'ie_ieprogramid'] = np.nan


# In[8]:

# Completamos la información básica del programa con atributos de su tipo de programa

ie_programtype_attributes = ['ie_programtypeid', 'ie_ieprogramtypeid', 'ie_name', 'ie_name_es', 
                             'ie_programcategory_displayname' ]

ie_programtype = pd.read_csv('s3://iedatalakelanding/crm/IE_ProgramType.csv.gz', sep = ';', 
                             compression = 'gzip', quotechar='|', quoting=csv.QUOTE_MINIMAL, 
                             usecols = ie_programtype_attributes)

# Atribución del literal 'N/A' para categorías no disponibles en el tipo de programa

ie_programtype['ie_programcategory_displayname'].fillna('N/A', inplace = True)

# Por si acaso creasen algún tipo sin nombre, imputamos el del idioma contrario si hubiera, 
# y si no tiene valor, el literal 'N/A'

ie_programtype['ie_name'] = ie_programtype['ie_name'].fillna(ie_programtype['ie_name_es'])
ie_programtype['ie_name'] = ie_programtype['ie_name'].fillna('N/A')

# Eliminamos el nombre en español, que sólo hemos usado para completar si viene vacío en inglés

ie_programtype = ie_programtype.drop(['ie_name_es'], axis = 1)

ie_program = pd.merge(ie_program, ie_programtype, how = 'left',                       on = ['ie_programtypeid', 'ie_programtypeid'])

del ie_programtype

# Renombramos 'ie_name' con el nombre del tipo de programa para evitar confusiones

ie_program = ie_program.rename(columns = {'ie_name':'ie_programtype_name'})



# In[9]:

# Añadimos las escuelas asociadas a cada programa. 
# Esto introducirá multiplicidad al poder haber varias.

ie_program_schools = pd.read_csv('s3://iedatalakelanding/crm/IE_IE_IESchool_IE_Program.csv.gz', 
                                 sep = ';', compression = 'gzip', quotechar='|', 
                                 quoting=csv.QUOTE_MINIMAL)

ie_ieschool = pd.read_csv('s3://iedatalakelanding/crm/IE_IESchool.csv.gz', 
                        sep = ';', compression = 'gzip', quotechar='|', quoting=csv.QUOTE_MINIMAL)

ie_prog_schools = pd.merge(ie_program_schools, ie_ieschool, 
                           on = ['ie_ieschoolid', 'ie_ieschoolid']) [['ie_programid', 'ie_name']]

ie_program = pd.merge(ie_program, ie_prog_schools, how = 'left', on = ['ie_programid', 'ie_programid'])

del ie_program_schools

ie_program = ie_program.rename(columns = {'ie_name':'ie_ieschool'})

# Por si hubieran asignado varias veces una escuela a un programa, eliminamos posibles duplicados

ie_program = ie_program.drop_duplicates()


# In[10]:

# Añadimos la agrupación a la que pertenece el programa, con su identificador al haber homonimia y
# solapamiento

ie_agrupation = pd.read_csv('s3://iedatalakelanding/crm/IE_Agrupation.csv.gz', sep = ';', 
                compression = 'gzip', quotechar='|', quoting=csv.QUOTE_MINIMAL)

ie_prog_agrupation = pd.merge(ie_program, ie_agrupation, how = 'left',                      on = ['ie_agrupationid', 'ie_agrupationid']) [['ie_programid', 'ie_name']]                      .drop_duplicates()
    
ie_program = pd.merge(ie_program, ie_prog_agrupation, how = 'left',                       on = ['ie_programid', 'ie_programid']).drop_duplicates()

del ie_agrupation

ie_program = ie_program.rename(columns = {'ie_name':'ie_agrupation'})

# Imputamos la agrupación 'N/A' con identificador '4F4C54C3-4A70-4588-8F11-C5E919A9CE75' a los nulos

ie_program['ie_agrupation'] = ie_program['ie_agrupation'].fillna('N/A')

ie_program['ie_agrupationid'] =                         ie_program['ie_agrupationid'].fillna('4F4C54C3-4A70-4588-8F11-C5E919A9CE75')


# In[11]:

# Traducción de atributos de listas de valor que no se replican traducidos en un campo display_name: 

stringmap = pd.read_csv('s3://iedatalakelanding/crm/StringMap.csv.gz', sep = ';', 
                        compression = 'gzip', quotechar='|', quoting=csv.QUOTE_MINIMAL) \
                        [['attributename', 'attributevalue', 'value', 'objecttypecode', 'langid']]

# ie_academiccatalog

tr_ie_academiccatalog = stringmap[ (stringmap['objecttypecode'] == 'ie_program') &                        (stringmap['langid'] == 1033) &                        (stringmap['attributename'] == 'ie_academiccatalog') ] 
ie_prog_acadcat = pd.merge(ie_program, tr_ie_academiccatalog, left_on = 'ie_academiccatalog', 
                              right_on = 'attributevalue', how  ='left') [['ie_programid', 'value']] \
                           .drop_duplicates()
ie_prog_acadcat = ie_prog_acadcat.rename(columns = {'value':'ie_academiccatalog_displayname'})
ie_program = pd.merge(ie_program, ie_prog_acadcat, how = 'left',                       on = ['ie_programid', 'ie_programid']).drop_duplicates()

# ie_comercialcatalog

tr_ie_comercialcatalog = stringmap[ (stringmap['objecttypecode'] == 'ie_program') &                        (stringmap['langid'] == 1033) &                        (stringmap['attributename'] == 'ie_comercialcatalog') ] 
ie_prog_comcat = pd.merge(ie_program, tr_ie_comercialcatalog, left_on = 'ie_comercialcatalog', 
                          right_on = 'attributevalue', how = 'left') [['ie_programid', 'value']] \
                         .drop_duplicates()
ie_prog_comcat = ie_prog_comcat.rename(columns = {'value':'ie_comercialcatalog_displayname'})
ie_program = pd.merge(ie_program, ie_prog_comcat, how = 'left',                       on = ['ie_programid', 'ie_programid']).drop_duplicates()


# ie_programtype (tipo de programa para el antiguo Sistema Plus)

tr_ie_programtype = stringmap[ (stringmap['objecttypecode'] == 'ie_program') &                        (stringmap['langid'] == 1033) &                        (stringmap['attributename'] == 'ie_programtype') ]
ie_prog_progtype = pd.merge(ie_program, tr_ie_programtype, left_on = 'ie_programtype', 
                            right_on = 'attributevalue', how = 'left') [['ie_programid', 'value']] \
                           .drop_duplicates()
ie_prog_progtype = ie_prog_progtype.rename(columns = {'value':'ie_programtype_displayname'})
ie_program = pd.merge(ie_program, ie_prog_progtype, how = 'left',                       on = ['ie_programid', 'ie_programid']).drop_duplicates()


# ie_untrueprogram (marca de si el programa es ficticio), por defecto imputaremos 'No'

tr_ie_untrue = stringmap[ (stringmap['objecttypecode'] == 'ie_program') &                        (stringmap['langid'] == 1033) &                        (stringmap['attributename'] == 'ie_untrueprogram') ]
ie_prog_untrue = pd.merge(ie_program, tr_ie_programtype, left_on = 'ie_untrueprogram', 
                            right_on = 'attributevalue', how = 'left') [['ie_programid', 'value']] \
                           .drop_duplicates()
ie_prog_untrue = ie_prog_untrue.rename(columns = {'value':'ie_untrueprogram_displayname'})
ie_prog_untrue['ie_untrueprogram_displayname'].fillna('No', inplace = True)
ie_program = pd.merge(ie_program, ie_prog_untrue, how = 'left',                       on = ['ie_programid', 'ie_programid']).drop_duplicates()

# ie_commercialized

tr_ie_commercialized = stringmap[ (stringmap['objecttypecode'] == 'ie_program') &                        (stringmap['langid'] == 1033) &                        (stringmap['attributename'] == 'ie_commercialized') ] 
ie_prog_com = pd.merge(ie_program, tr_ie_commercialized, left_on = 'ie_commercialized', 
                       right_on = 'attributevalue', how = 'left') [['ie_programid', 'value']] \
                        .drop_duplicates()
ie_prog_com = ie_prog_com.rename(columns = {'value':'ie_commercialized_displayname'})
ie_program = pd.merge(ie_program, ie_prog_com, how = 'left',                       on = ['ie_programid', 'ie_programid']).drop_duplicates()

# Borrado de columnas previas a la traducción

ie_program = ie_program.drop(['ie_academiccatalog', 'ie_comercialcatalog', 'ie_programtype',                               'ie_commercialized', 'ie_untrueprogram'], axis = 1)


# In[12]:

# Prevalencia de denominación Core asociado para programas provenientes de 
# combinaciones de Core + Specialization del antiguo sistema Plus

ie_progtocore  = pd.read_csv('s3://iedatalakelanding/crm/IE_ProgramToProgramCore.csv.gz', 
                 sep = ';', compression = 'gzip', quotechar='|', quoting=csv.QUOTE_MINIMAL)

cores = pd.merge(ie_program, ie_progtocore, left_on = 'ie_programid', 
         right_on = 'ie_pogramcatalog')[['ie_programid', 'ie_coreid']].drop_duplicates()

cores = cores.rename(columns = {'ie_programid':'ie_programidSC'})

cores = pd.merge(cores, ie_program, left_on='ie_coreid', right_on = 'ie_programid')         [['ie_coreid', 'ie_programidSC', 'ie_shortname', 'ie_longname' ]].drop_duplicates()

cores = cores.rename(columns = {'ie_programidSC':'ie_programid',                                 'ie_shortname':'ie_shortname_core', 
                                'ie_longname':'ie_longname_core'})

ie_program = pd.merge(ie_program, cores, how = 'left',                       on = ['ie_programid', 'ie_programid']).drop_duplicates()

ie_program.loc[ie_program['ie_coreid'].notnull(), 'ie_shortname'] = ie_program['ie_shortname_core']

ie_program.loc[ie_program['ie_coreid'].notnull(), 'ie_longname'] = ie_program['ie_longname_core']

# Borrado de columnas auxiliares para cores

ie_program = ie_program.drop(['ie_coreid', 'ie_shortname_core', 'ie_longname_core'],                               axis = 1)


# In[13]:

ie_program = ie_program.rename(columns = {'ie_programid':'ProgramId', 
                                         'ie_location_displayname':'Location', 
                                         'createdon':'CreationDate', 
                                         'ie_dirigidoa':'AimedAt',
                                         'ie_languageidname':'Language',                                         
                                         'ie_studytype_displayname':'StudyType',
                                         'ie_program_crm_label':'CRMLabel',
                                         'ie_shortname':'ShortName',
                                         'ie_ieprogramid':'CorporateProgramId',
                                         'ie_campusplace_displayname':'Campus',
                                         'statecode_display_name':'Status', 
                                         'ie_format_displayname':'Format',
                                         'ie_directoridname':'Director',
                                         'ie_longname':'LongName',
                                         'ie_programtypeid':'ProgramTypeId',
                                         'ie_secretaryidname':'Secretary',
                                         'ie_belongto_displayname':'BelongTo',
                                         'ie_agrupationid': 'ProgramGroupId',
                                         'ie_ieprogramtypeid': 'CorporateProgramTypeId',                                         
                                         'ie_programcategory_displayname':'ProgramCategory',
                                         'ie_programtype_name':'ProgramType', 
                                          'ie_ieschool':'School', 
                                          'ie_agrupation':'ProgramGroup',
                                          'ie_academiccatalog_displayname':'IsInAcademicCatalog', 
                                          'ie_comercialcatalog_displayname':'IsInCommercialCatalog',
                                          'ie_programtype_displayname':'PlusSystemProgramType', 
                                          'ie_untrueprogram_displayname':'IsUntrueProgram',
                                          'ie_commercialized_displayname':'Commercialized'                                          
                                         })


# In[14]:

## Carpeta auxiliar para depositar los CSVs generados antes de subir

if not os.path.exists("crm-data-in-shape"):
    os.makedirs("crm-data-in-shape")

csvFile = "crm-data-in-shape/Program.csv.gz"
ie_program.to_csv(csvFile, sep = ';', compression = 'gzip', quotechar='|', quoting=csv.QUOTE_MINIMAL, 
                 encoding = 'utf8', mode= 'a', header = 'false', line_terminator = '\n\r')     


# In[15]:

## Sincronización de carpeta local con carpeta análoga del bucket S3 iedatalakeprocess
## Hace uso de AWSCLI (previamente instalado mediante la instrucción: sudo apt install awscli)

transformationFinalDate = str(datetime.datetime.now())
os.system('aws  s3 sync crm-data-in-shape s3://iedatalakeprocess/crm-data-in-shape --metadata transformationprocess="' 
          + transformationProcess + '",transformationversion="' + transformationVersion 
          + '",transformationinitialdate="' + transformationInitialDate 
          + '",transformationfinaldate="' + transformationFinalDate + '",code="Transform-Program-to-process"');


# In[ ]:



