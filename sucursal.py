#--------------------------#
# Importacion de Librerías #
#--------------------------#

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

# Conexion a la base de datos MYSQL
my_conn = create_engine(f"mysql+mysqldb://{user}:{password}@{host}/{db_name}")
BASE_PATH = './Datasets/'
#-------------------#
# Limpieza de datos #
#-------------------#

def limpieza_sucursal(df):
    # Se elimina los duplicados
    if df.duplicated().sum().sum() > 0:
        print(f'Valores duplicados:\n{df.duplicated().sum()}')
        df = df.drop_duplicates()
    # Verificación de valores nulos
    elif df.isna().sum().sum() > 0:
        print(f'Valores faltantes/nulos por cada columna:\n{df.isna().sum()}\n')
        df = df.dropna()
    else:
        print('No hay valores nulos ni repetidos...')
    print('No hay valores nulos ni repetidos...')
    df = df.rename(columns={
        'id':'idSucursal', 
        'comercioId':'idComercio', 
        'banderaId':'idBandera', 
        'banderaDescripcion':'banderaDescripcion',
        'comercioRazonSocial':'comercioRazonSocial',
        'provincia':'provincia', 
        'localidad':'localidad', 
        'direccion':'direccion', 
        'lat':'lat',
        'lng':'lng', 
        'sucursalNombre':'sucursalNombre', 
        'sucursalTipo':'sucursalTipo'})
    return df

#-------------------------------#
# Creacion de la Tabla Sucursal #
#-------------------------------#

def crear_tabla_sucursal(tb_name):
    try:
        conn = msql.connect(
            host=host,
            user=user, 
            password=password)

        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name};")
            cursor.execute(f"USE {db_name};")
            record = cursor.fetchone()
            #
            print("You're connected to database: ", db_name)
            #
            cursor.execute(f'DROP TABLE IF EXISTS `{tb_name}`;')
            print(f'Creando la tabla {tb_name}....')
            #
            cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS `{tb_name}` (
                `idSucursal`			VARCHAR(20) NOT NULL,
                `idComercio` 			INTEGER NOT NULL,
                `idBandera`				INTEGER NOT NULL,
                `banderaDescripcion`	VARCHAR(150) COLLATE utf8mb4_spanish_ci NOT NULL,
                `comercioRazonSocial`	VARCHAR(150) COLLATE utf8mb4_spanish_ci NOT NULL,
                `provincia`				VARCHAR(4) NOT NULL,
                `localidad`				VARCHAR(100) NOT NULL,
                `direccion`				VARCHAR(150) NOT NULL,
                `lat`					VARCHAR(19),
                `lng`					VARCHAR(19),
                `sucursalNombre`		VARCHAR(100),
                `sucursalTipo`			VARCHAR(12),
                PRIMARY KEY (`idSucursal`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
            """)
            print(f"Tabla {tb_name} creada exitosamente....")
    except Error as e:
                print("Error al conectarse a MySQL", e)

def cargar_tabla():    
    file_path = input('Ingrese el nombre del archivo. Ejemplo archivo.extensio: ')    
    extension_archivo = os.path.basename(file_path).split('.')[1]
    
    if ('precio' in file_path) and (extension_archivo == 'csv' or 'txt'):
        df = pd.read_csv(BASE_PATH + file_path, encoding='utf_16_le')
        nombre_tabla = 'precio'        
        
    elif ('precio' in file_path)and (extension_archivo == 'xlsx'):
        df = pd.read_excel(BASE_PATH + file_path)
        nombre_tabla = 'precio'
        
    elif ('precio' in file_path) and (extension_archivo == 'json'):
        df = pd.read_json(BASE_PATH + file_path)
        nombre_tabla = 'precio'
    return df

def leer_archivo():
    file_path = input('Ingrese el nombre del archivo. Ejemplo archivo.extensio: ')    
    extension_archivo = os.path.basename(file_path).split('.')[1]
    data = pd.read_csv(BASE_PATH + file_path)
    data_limpia = limpieza_sucursal(data)
    return data_limpia

def main():
    df = leer_archivo()
    tb_name = 'sucursal'
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
                    `idProducto`		VARCHAR(20),
                    `marca` 			VARCHAR(50),
                    `nombre`			VARCHAR(150),
                    `presentacion`		VARCHAR(150),
                    `categoria1`		VARCHAR(50),
                    `categoria2`		VARCHAR(50),
                    `categoria3`		VARCHAR(50),
                    PRIMARY KEY (`idProducto`)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_spanish_ci;
            """)
            print("Tabla creada exitosamente....")
            df.to_sql(con=my_conn, name=tb_name, if_exists='append',index=False)
            print("Los registros se han cargado exitosamente....")
            
    except Error as e:
        print("Error while connecting to MySQL", e)


if __name__ == '__main__':
    main()
    
    
