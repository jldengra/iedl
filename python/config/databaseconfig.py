# Configuration file for database connections in python scripts

# dialect+driver://username:password@host:port/database[?driver=especificdriver]

# Microsoft Dynamics CRM Online Scribe Replication
# 'mssql+pyodbc://cloudcrm:2013cloud2013@10.69.1.154:1433/IE_Vistas?driver=ODBC+Driver+13+for+SQL+Server'
crm = {'dialect': 'mssql', 
       'driver': 'pyodbc',
       'username': 'cloudcrm', 
       'password': '2013cloud2013',
       'host': '10.69.1.154:1433', 
       'database': 'IE_Vistas',
       'parameters': '?driver=ODBC+Driver+13+for+SQL+Server'}

# Microsoft Dynamics AX
# 'mssql+pyodbc://jupyter:IEDataLake1@AXSQLCORP.ie.es\AXSQL:52040/PRO2?driver=ODBC+Driver+13+for+SQL+Server'
ax = {'dialect': 'mssql', 
      'driver': 'pyodbc',
      'username': 'jupyter', 
      'password': 'IEDataLake1',
      'host': 'AXSQLCORP.ie.es\AXSQL:52040', 
      'database': 'PRO2',
      'parameters': '?driver=ODBC+Driver+13+for+SQL+Server'}

# Online Application (Solicitud Online)
# 'mssql+pyodbc://sa:eqlmdj,mevl@10.69.5.80:1433/SOLICITUD_ONLINE_2017?driver=ODBC+Driver+13+for+SQL+Server'
oa = {'dialect': 'mssql', 
      'driver': 'pyodbc',
      'username': 'sa', 
      'password': 'eqlmdj,mevl',
      'host': '10.69.5.80:1433', 
      'database': 'SOLICITUD_ONLINE_2017',
      'parameters': '?driver=ODBC+Driver+13+for+SQL+Server'}
