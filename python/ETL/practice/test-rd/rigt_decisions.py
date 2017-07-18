
# coding: utf-8

# # Autor: Magali Valdepérez Pastor

# # VERSIONES EMPLEADAS

# In[1]:

import platform
print('Python version: '+ platform.python_version())
import numpy as np
print ('Numpy version: '+ np.__version__)
import pandas as pd
print ('Pandas version: '+ pd.__version__ + '\n')


# # PROBLEMA A PRESENTAR:
# A partir de una base de datos relacional de una empresa especializada en venta de repertorios musicales, se va a emular una ingesta y transformación de datos distribuidos usando ficheros CSV programando en python y utilizando DataFrames de pandas. Es una base de datos de ejemplo en SQLite tomada de la página web http://www.sqlitetutorial.net/sqlite-sample-database y viene adjunta en un archivo BaseDatosEjemplo.rar que contiene el fichero de datos llamado chinook.db y dos documentos PDF con el diagrama explicativo del esquema lógico de la base de datos.

# # 1. Ingesta
# En una primera fase de adquisición de datos, se necesita leer la información en bruto de todas las tablas de la base de datos en DataFrames de pandas y llevar cada una de ellas a un fichero CSV dentro de una carpeta  llamada ingesta. Los ficheros generados deberán llevar el mismo nombre que la tabla origen, incorporar una primera línea de cabecera con los nombres de columnas tal como vienen en el esquema, llevar el formato de compresión gzip y extensión "csv.gz", separador punto y coma, y codificación de caracteres UTF-8.
# 
# Se conoce que las tablas artists y tracks van a tener siempre muchos más registros que las otras, y en algún momento pueden dar problema de llenado de memoria si se intentan cargar enteras en un DataFrame para su ingesta. Por ello no puede programarse su carga y volcado a fichero en un solo paso, sino que debe plantearse en bloques y de forma iterativa. Para ello, suponer que el sistema no es capaz de soportar una carga simultánea en memoria de más de 100 filas de estas dos tablas.

# # OBJETIVO 1: 
# Obtener 1 csv por tabla con los datos en bruto. La primera fila con las cabeceras. Como punto adicional, plantear cómo se abordaría el problema de cargar el fichero por bloques por su problema de tamaño. Mostrar la programación que se haya realizado.

# In[2]:

#Gestión de carpetas

import os

def create_subdir(dir):
    try:
        os.stat(dir)
    except:
        os.mkdir(dir)
        
my_dir=os.getcwd()
files_dir=my_dir+"/ingesta"
transfo_dir=my_dir+"/transformacion"

create_subdir(files_dir), create_subdir(transfo_dir)        


# In[3]:


import sqlite3

def get_header(table_name):
    table_header=[]
    metadata=cursor.execute("PRAGMA table_info("+table_name+")")
    for data in metadata:
        table_header.append(data[1])       
    return table_header    

def save_csv_data(my_table):
    table_header=get_header(my_table)      
    data_query="SELECT * from "+my_table+" ;"
    table_data=pd.read_sql_query(data_query,conn)
    os.chdir(files_dir)
    table_data.to_csv(my_table+'.csv.gz', header=table_header,index=False, sep=';',encoding='utf-8',compression='gzip')    
    
#Connect to database
database='/opt/rd/db/chinook.db'
conn = sqlite3.connect(database)

#Get the list of tables
sqlite_table_names=['sqlite_sequence','sqlite_stat1']
special_tables=['artists','tracks']
names_to_delete=sqlite_table_names+special_tables

cursor = conn.cursor()
list_query="SELECT name FROM sqlite_master WHERE type='table';" 
cursor.execute(list_query)
tables = cursor.fetchall()

all_table_names=[table[0] for table in tables]
for name_to_delete in names_to_delete:
    all_table_names.pop(all_table_names.index(name_to_delete))
table_names=all_table_names  

#Dump tables
for name in table_names:
    save_csv_data(name)
    os.chdir(my_dir)

#Special Tables
for name in special_tables:
    count_query="SELECT COUNT(*) FROM "+name+";"
    cursor.execute(count_query)
    number_of_rows=cursor.fetchall()[0][0]
    iterations=number_of_rows//100
    offset=0
    if number_of_rows%100>0:   
        for i in range(iterations+2):            
            data_query="SELECT * FROM "+name+ " LIMIT 100 OFFSET "+str(offset)+";"
            if offset==0:
                save_csv_data(name)
            else:
                table_data=pd.read_sql_query(data_query,conn)
                table_data.to_csv(name+'.csv.gz', mode='a', header=False,index=False, sep=';',encoding='utf-8',compression='gzip')
            offset=offset+100
os.chdir(my_dir)   


# # Transformación y combinación de datos
# En una segunda fase de manipulación de datos, se necesita combinar la información de varios de los ficheros obtenidos para generar otros CSVs con los datos transformados dentro de otra carpeta llamada transformacion, en un fichero canciones.csv.gz, en el mismo formato que los anteriores, con estos datos a modo de tablón para reunir datos de canciones para que puedan servir como origen para analíticas e informes, incluyendo estas columnas:
# 
# IdCancion, NombreAlbum, NumeroPista, NombreCancion, Interprete, GeneroMusical, TipoMedio, NombreCompletoCliente, Empresa, NumeroFactura, NumeroLineaFactura, PrecioUnitario, Cantidad 
# 
# Las columnas IdCancion (TrackId), NumeroPista, PrecioUnitario y Cantidad llevarán valores numéricos y el resto textos que habrá que relacionar y traducir como resultado de cargar y relacionar DataFrames de pandas a partir de los ficheros albums.csv.gz, artists.csv.gz, customers.csv.gz, genres.csv.gz, invoices.csv.gz, invoice_items.csv.gz, media_types.csv.gz y tracks.csv.gz generados en la fase de adquisición de datos, gestionando duplicados. 
# 
# La columna NombreAlbum debe llevar todo el texto en mayúscula, Titulo y Cantante deben llevar el texto capitalizado (la primera letra de cada palabra en mayúsculas), y deben eliminarse espacios en blanco a izquierda y derecha de todos los textos obtenidos para todas las columnas. La columna NumeroPista no viene en los ficheros origen, sino que debe calcularse como el orden correlativo, comenzando en 1, de los identificadores de canciones para un mismo Album. El campo NombreCompletoCliente debe obtenerse como la concatenación del primer nombre y apellido del cliente capitalizados dejando un espacio entre ellos, y el campo Empresa deberá llevar el literal ‘N/A’ en los casos en que no venga informado en el origen.

# # OBJETIVO 2: 
# Obtener un único CSV con las transformaciones descritas. Mostar la programación que se haya realizado

# In[4]:

canciones_header=['IdCancion', 'NombreAlbum', 'NumeroPista', 'NombreCancion', 'Interprete', 'GeneroMusical', 'TipoMedio', 'NombreCompletoCliente', 'Empresa', 'NumeroFactura', 'NumeroLineaFactura', 'PrecioUnitario', 'Cantidad']

files_list=['albums.csv.gz','artists.csv.gz', 'customers.csv.gz', 'genres.csv.gz', 'invoices.csv.gz', 'invoice_items.csv.gz', 'media_types.csv.gz', 'tracks.csv.gz']
pd_names=[ x.replace('.csv.gz','') for x in files_list]

os.chdir(files_dir)

#Multiple creation of dataframes
for i in range(len(files_list)):
    vars()[pd_names[i]]=pd.read_csv(files_list[i],sep=';',encoding='utf-8',compression='gzip')



# ### Cálculo del número de pista 

# In[5]:


columns_to_keep_from_tracks=['TrackId','Name','AlbumId','MediaTypeId','GenreId','UnitPrice']

tracks_new=tracks[columns_to_keep_from_tracks]
max_album_id=tracks_new['AlbumId'].max()

sorted_tracks=tracks_new.sort_values(by=['AlbumId','TrackId'], ascending=True)

#Add and empty column called 'NumeroPista'
new_cols=['TrackId','Name','AlbumId','MediaTypeId','GenreId','UnitPrice','NumeroPista']
tracks_pista = sorted_tracks.reindex(columns = new_cols)  

#Grouping by values
group= tracks_pista.groupby( 'AlbumId' )

df_list=[]
for i in range(1,max_album_id):
    df=group.get_group(i)
    df_calc = df.assign(NumeroPista=[i+1 for i in range(len(df))])
    df_list.append(df_calc)
    del df

tracks_calc=pd.concat(df_list,axis=0)
tracks_calc.head()


# ### Transformación de datos

# In[6]:

#Some cleaning before joining data

columns_to_keep_invoice_items=['InvoiceId','TrackId','UnitPrice','Quantity','InvoiceLineId']
columns_to_keep_invoices=['InvoiceId','CustomerId']
columns_to_keep_customers=['CustomerId','FirstName','LastName','Company']


invoice_items_new=invoice_items[columns_to_keep_invoice_items]
invoices_new=invoices[columns_to_keep_invoices]
customers_new=customers[columns_to_keep_customers]


#Product info
albums_artists_join=albums.join(artists.set_index('ArtistId'), on='ArtistId')
albums_artists_join = albums_artists_join.rename(columns={'Title': 'NombreAlbum', 'Name': 'Interprete'})

tracks_artists_join=tracks_calc.join(albums_artists_join.set_index('AlbumId'), on='AlbumId')
tracks_artists_join=tracks_artists_join.rename(columns={'Name': 'NombreCancion'})
cleaned_track=tracks_artists_join.drop(['AlbumId','ArtistId'],axis=1) 
media_join=cleaned_track.join(media_types.set_index('MediaTypeId'), on='MediaTypeId')
media_join = media_join.rename(columns={'Name': 'TipoMedio'})
cleaned_media=media_join.drop('MediaTypeId',axis=1)
genre_join=cleaned_media.join(genres.set_index('GenreId'), on='GenreId')
genre_join = genre_join.rename(columns={'Name': 'GeneroMusical'})
product=genre_join.drop('GenreId',axis=1)

#Purchase info
invoices_join=invoice_items_new.join(invoices_new.set_index('InvoiceId'), on='InvoiceId')
invoices_join=invoices_join.rename(columns={'UnitPrice': 'PrecioUnitario', 'Quantity': 'Cantidad', 'InvoiceId': 'NumeroFactura', 'InvoiceLineId': 'NumeroLineaFactura'})
customers_join=invoices_join.join(customers_new.set_index('CustomerId'),on='CustomerId')
customers_join=customers_join.rename(columns={'Company': 'Empresa'})
cleaned_customers=customers_join.drop(['CustomerId','PrecioUnitario'],axis=1)
purchase_df=cleaned_customers
purchase_df['NombreCompletoCliente']=purchase_df['FirstName']+' '+purchase_df['LastName']
purchase=purchase_df.drop(['FirstName','LastName'],axis=1)

raw_result=product.join(purchase.set_index('TrackId'), on='TrackId')
raw_result=raw_result.rename(columns={'TrackId': 'IdCancion'})

#Output data formatting
raw_result=raw_result.rename(columns={'UnitPrice': 'PrecioUnitario'})
raw_result.Empresa=raw_result.Empresa.fillna('N/A')
raw_result['NombreAlbum']=raw_result['NombreAlbum'].str.upper()

#str_result=raw_result.to_string(justify='right')
#No me ha dado tiempo a:
#Quitar espacios en blanco con lstrip rstrip
#Por el problema:"IOPub data rate exceeded.
#The notebook server will temporarily stop sending output
#to the client in order to avoid crashing it.
#To change this limit, set the config variable
#`--NotebookApp.iopub_data_rate_limit`.
#"

#Order cols
canciones_df=raw_result[canciones_header]
canciones_df.head()




# ### Resultado en formato csv

# In[7]:

os.chdir(transfo_dir)
canciones_df.to_csv('canciones.csv.gz', header=canciones_header,index=False, sep=';',encoding='utf-8',compression='gzip')
os.chdir(my_dir)

