
# Importacion de Librerías
import pandas as pd
import numpy as np

import mysql.connector as msql
from mysql.connector import Error

from sqlalchemy import create_engine

from decouple import config
import os
#-------------------------------------------------#
# Configuracion de la conexión a la base de datos #
#-------------------------------------------------#

host=config('MYSQL_HOST')
user=config('MYSQL_USER')
password=config('MYSQL_PASSWORD')
db_name=config('MYSQL_DB')
BASE_PATH = './Datasets/'

# Conexion a la base de datos MYSQL
my_conn = create_engine(f"mysql+mysqldb://{user}:{password}@{host}/{db_name}")

precios_abril_13=pd.read_csv('./Datasets/precios_semana_20200413.csv', encoding='utf_16_le')
precios_abril_19=pd.read_excel('./Datasets/precios_semanas_20200419_20200426.xlsx')
precios_mayo_03=pd.read_json('./Datasets/precios_semana_20200503.json')
precios_mayo_18=pd.read_csv('./Datasets/precios_semana_20200518.txt', sep='|')

#-------------------#
# Limpieza de datos #
#-------------------#

def limpieza_precio(df):
    # Se elimina los duplicados
    if df.duplicated().sum().sum() > 0:
        print(f'Valores duplicados:\n{df.duplicated().sum()}')
        df = df.drop_duplicates()
        print('Valores duplicados borrados')
    # Verificación de valores nulos
    elif df.isna().sum().sum() > 0:
        print(f'Valores faltantes/nulos por cada columna:\n{df.isna().sum().sum()}\n')
        df = df.dropna(subset=['producto_id','sucursal_id'])
        print('Valores nulos borrados')
        df = df['precio'].fillna(0)
    else:
        print('No hay valores nulos ni repetidos...')
    df = df.rename(columns={
        'precio':'precio', 
        'producto_id':'idProducto', 
        'sucursal_id':'idSucursal'})
    
    return df

#-----------------------------#
# Creacion de la Tabla Precio #
#-----------------------------#

def crear_tabla_precio(tb_name):
    tb_name='precio'
    try:
        conn = msql.connect(
            host=host,
            user=user, 
            password=password)

        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
            cursor.execute("USE henrypi;")
            record = cursor.fetchone()
            #
            print("You're connected to database: ", db_name)
            #
            cursor.execute(f'DROP TABLE IF EXISTS `{tb_name}`;')
            print(f'Creando la tabla {tb_name}....')
            #
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{tb_name}` (
                `precio`		VARCHAR(25),
                `idProducto`	VARCHAR(18),
                `idSucursal`	VARCHAR(15)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
            """)
            print("Tabla creada exitosamente....")
    except Error as e:
        print("Error al conectarse a MySQL", e)

def leer_archivo(): 
    file_path = input('Ingrese el nombre del archivo. Ejemplo archivo.extensio: ')    
    extension_archivo = os.path.basename(file_path).split('.')[1]
    
    if (extension_archivo == 'csv') & ('precio' in file_path):
        data = pd.read_csv(BASE_PATH + file_path, encoding='utf_16_le')
        data_limpia = limpieza_precio(data)
    elif (extension_archivo == 'txt') & ('precio' in file_path):
        data = pd.read_csv(BASE_PATH + file_path)
        data_limpia = limpieza_precio(data)
    elif (extension_archivo == 'xlsx') & ('precio' in file_path):
        data = pd.read_excel(BASE_PATH + file_path)
        data_limpia = limpieza_precio(data)
    elif ('precio' in file_path) & (extension_archivo == 'json'):
        data = pd.read_json(BASE_PATH + file_path)
        data_limpia = limpieza_precio(data)
    return data_limpia

def main():
    df = leer_archivo()
    tb_name = 'precio'
    try:
        conn = msql.connect(
            host=host,
            user=user, 
            password=password)

        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
            cursor.execute("USE henrypi;")
            record = cursor.fetchone()
            #
            print("You're connected to database: ", db_name)
            #
            cursor.execute(f'DROP TABLE IF EXISTS `{tb_name}`;')
            print('Creating table....')
            #
            cursor.execute("""
            CREATE TABLE IF NOT EXISTS `producto` (
                `precio`		VARCHAR(25),
                `idProducto`	VARCHAR(18),
                `idSucursal`	VARCHAR(15)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
            """)
            print("Tabla creada exitosamente....")
            df.to_sql(con=my_conn, name=tb_name, if_exists='append',index=False)
            print("Los registros se han cargado exitosamente....")
            
    except Error as e:
        print("Error while connecting to MySQL", e)


if __name__ == '__main__':
    main()
    
    