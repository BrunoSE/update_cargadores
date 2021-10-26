#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import numpy as np
import MySQLdb
import logging
from datetime import datetime, timedelta
global logger
global file_format


def mantener_log():
    '''Funcion que define un objeto que printea a la consola mensajes
    que se guardan a un log detallando proceso, linea del codigo y timestamp.
    '''
    global logger
    global file_format
    logger = logging.getLogger(__name__)  # P: número de proceso, L: número de línea
    logger.setLevel(logging.DEBUG)  # deja pasar todos desde debug hasta critical
    print_handler = logging.StreamHandler()
    print_format = logging.Formatter('[{asctime:s}] {levelname:s} L{lineno:d}| {message:s}',
                                     '%Y-%m-%d %H:%M:%S', style='{')
    file_format = logging.Formatter('[{asctime:s}] {processName:s} P{process:d}@{name:s} ' +
                                    '${levelname:s} L{lineno:d}| {message:s}',
                                    '%Y-%m-%d %H:%M:%S', style='{')
    # printear desde debug hasta critical:
    print_handler.setLevel(logging.DEBUG)
    print_handler.setFormatter(print_format)
    logger.addHandler(print_handler)


def query_data_diaria(fecha_str_ayer, fecha_str_hoy):
    # Query entrega 24 horas de data telemetria de cargadores a partir de las 7:30am de un dia
    db1 = MySQLdb.connect(host="192.168.11.150",
                          user="brunom",
                          passwd="Manzana",
                          db="stp_estacionamiento")

    cur1 = db1.cursor()

    query1 = ( f"""
                SELECT * FROM (
                    SELECT
                        min(id) AS id, marquesina_id, marquesina_nombre, cargador_id, cargador_nombre,
                        pistola_id, pistola_nro, power_active_import, energy_active_import_register,
                        voltage, fecha_hora_evento, min(fecha_hora_consulta) AS fecha_hora_consulta,
                        soc, temperatura, current_import, fecha_hora_inicio_valores_copec,
                        fecha_hora_termino_valores_copec
                    FROM
                        stp_estacionamiento.cargadores_historico
                    WHERE
                        power_active_import > '20' AND
                        voltage <> '0' AND
                        current_import <> '0' AND
                        soc <= '99' AND
                        fecha_hora_evento BETWEEN '{fecha_str_ayer} 07:30:01' AND '{fecha_str_hoy} 07:30:00'
                    GROUP BY
                        marquesina_id, marquesina_nombre, cargador_id, cargador_nombre,
                        pistola_id, pistola_nro, power_active_import, energy_active_import_register,
                        voltage, fecha_hora_evento,  soc, temperatura, current_import,
                        fecha_hora_inicio_valores_copec, fecha_hora_termino_valores_copec
                ) AS CHF
                LEFT JOIN
                (
                    SELECT
                        copeq_id AS pistola_id_copec, id AS pistola_id_stp, pistola_nombre
                    FROM
                        stp_estacionamiento.pistolas
                ) AS DICT
                ON CHF.pistola_id = DICT.pistola_id_copec;
                """
             )

    cur1.execute(query1)
    df1_ = pd.DataFrame([row for row in cur1.fetchall() if row[0] is not None],
                        columns=[i[0] for i in cur1.description])

    cur1.close()
    db1.close()

    return df1_



def query_reservas_diaria(fecha_str_ayer, fecha_str_hoy):
    # Query entrega 24 horas de data reservas a partir de las 7:30am de un dia
    db0 = MySQLdb.connect(host="192.168.11.150",
                          user="brunom",
                          passwd="Manzana",
                          db="stp_estacionamiento")

    cur0 = db0.cursor()

    query0 = ( f"""
                SELECT
                    id as reserva_id, pistola_id AS pistola_id_stp2, patente, fecha_hora_reserva, 
                    usuario_id, usuario_inicio_id, usuario_termino_id 
                FROM 
                    stp_estacionamiento.reservas 
                WHERE 
                    fecha_hora_reserva BETWEEN '{fecha_str_ayer} 07:30:01' AND '{fecha_str_hoy} 07:30:00';
                """
             )

    cur0.execute(query0)
    df0_ = pd.DataFrame([row for row in cur0.fetchall() if row[0] is not None],
                        columns=[i[0] for i in cur0.description])

    cur0.close()
    db0.close()

    return df0_


def main():
    mantener_log()
    fecha_hoy = datetime.today()
    fecha_ayer = fecha_hoy - timedelta(days=1)
    fecha_hoy = fecha_hoy.strftime('%Y-%m-%d')
    fecha_ayer = fecha_ayer.strftime('%Y-%m-%d')

    # -- Variables debug
    directorio = 'C:/Users/bruno/Desktop/Trabajo 2021/update_cargadores'
    fecha_hoy = '2021-09-28'
    fecha_ayer = '2021-09-27'
    do_query = True
    query_save = True
    query_load = False

    logger.info(f"Primer intento con fecha_ayer={fecha_ayer} y fecha_hoy={fecha_hoy}")

    if do_query:
        df_res = query_reservas_diaria(fecha_ayer, fecha_hoy)
        logger.info(f"Query reservas lista")
        df = query_data_diaria(fecha_ayer, fecha_hoy)
        logger.info(f"Query data cargadores lista")
    if query_save:
        df_res.to_parquet('df_res.parquet', compression='gzip')
        logger.info(f"Guardado parquet reservas")
        df.to_parquet('df.parquet', compression='gzip')
        logger.info(f"Guardado parquet data")
    if query_load:
        logger.info(f"Leyendo parquet reservas")
        df_res = pd.read_parquet(f'{directorio}/df_res.parquet')
        logger.info(f"Leyendo parquet data")
        df = pd.read_parquet(f'{directorio}/df.parquet')
    logger.info(f"Lista data")
    

if __name__ == '__main__':
    main()
