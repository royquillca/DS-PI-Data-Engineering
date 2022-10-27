# Importacion de Librerías
import pandas as pd
import numpy as np

import mysql.connector as msql
from mysql.connector import Error

from sqlalchemy import create_engine

from decouple import config
import os
# Importacion de módulos internos

from sucursal import limpieza_sucursal, crear_tabla_sucursal
from producto import limpieza_producto, crear_tabla_producto
from precio import limpieza_precio,  crear_tabla_precio


# Configuracion de la conexión a la base de datos
host=config('MYSQL_HOST')
user=config('MYSQL_USER')
password=config('MYSQL_PASSWORD')
db_name=config('MYSQL_DB')


# # Importación de arhivos
# precios_abril_13=pd.read_csv('./Datasets/precios_semana_20200413.csv', encoding='utf_16_le')
# precios_abril_19=pd.read_excel('./Datasets/precios_semanas_20200419_20200426.xlsx')
# precios_mayo_03=pd.read_json('./Datasets/precios_semana_20200503.json')
# precios_mayo_18=pd.read_csv('./Datasets/precios_semana_20200518.txt', sep='|')
# # 
# sucursal=pd.read_csv('./Datasets/sucursal.csv')
# producto=pd.read_parquet('./Datasets/producto.parquet')
# lista_precios = [precios_abril_13, precios_abril_19, precios_mayo_03, precios_mayo_18]

# Conexion a la base de datos MYSQL
my_conn = create_engine(f"mysql+mysqldb://{user}:{password}@{host}/{db_name}")




def crear_tablas():
    # ms = print("Escribe el nombre de la tabla que quieres cargar en singular.\nEjm: sucursal, producto, precio")
    nombre_tabla = input("Escribe el nombre de la tabla que quieres crear en singular.\nEjm: sucursal, producto, precio: ")
    creando = True
    while creando:
        print("""> Cerrar      : 0\n> Crear tabla : 1""")
        seleccion = input('Ingresa opcion: ')
        if seleccion == '1':
            if nombre_tabla == 'precio':
                crear_tabla_precio(nombre_tabla)            
            if nombre_tabla == 'producto':
                crear_tabla_producto(nombre_tabla)
            if nombre_tabla == 'sucursal':
                crear_tabla_sucursal(nombre_tabla)
        else:
            creando = False
    print(f'\nLa creacion de la tabla {nombre_tabla} ha finalizado exitosamente') 


if __name__ == '__main__':
    crear_tablas()
    