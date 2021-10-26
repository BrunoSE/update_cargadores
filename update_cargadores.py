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


def procesar_data(df, df_r):
    # asegurar id sea it, ordenar data antes de hacer analisis de secuencias
    df_r['reserva_id'] = df_r['reserva_id'].astype(int)
    df = df.sort_values(by=['pistola_id', 'fecha_hora_evento', 'soc'])

    # definir secuencias
    df['d_soc'] = df['soc'].shift(-1) - df['soc']
    df['dT'] = (df['fecha_hora_evento'].shift(-1) - df['fecha_hora_evento']) / pd.Timedelta(minutes=1)
    df['fin_secuencia'] = (((df['dT'] > 20) | (df['d_soc'] > 10)) | ((df['d_soc'] < 0) | (df['pistola_id'].shift(-1) != df['pistola_id'])))

    # asignar id unico a cada secuencia del dia
    df['inicio_secuencia'] = df['fin_secuencia'].shift(1)
    # primer valor de esta columna es NA, se reemplaza por True
    df['inicio_secuencia'].iloc[0:1].fillna(value=True, inplace=True)
    df['id_secuencia'] = df['inicio_secuencia'].astype(int).cumsum(axis = 0)

    # asignar tiempo inicial y final a cada secuencia
    dfg_ini = df[['id_secuencia', 'fecha_hora_evento']].groupby(by='id_secuencia').min()
    dfg_fin = df[['id_secuencia', 'fecha_hora_evento']].groupby(by='id_secuencia').max()

    dfg_ini.rename(columns={'fecha_hora_evento': 'tiempo_inicial_carga'}, inplace=True)
    dfg_fin.rename(columns={'fecha_hora_evento': 'tiempo_final_carga'}, inplace=True)

    dfg = dfg_ini.merge(dfg_fin, how='outer', left_index=True, right_index=True)
    df = df.merge(dfg, how='left', left_on='id_secuencia', right_index=True)

    # antes de merge_asof se requiere ordenar data en las llaves del join
    df.sort_values(by=['tiempo_inicial_carga'], inplace=True)
    df_r.sort_values(by=['fecha_hora_reserva'], inplace=True)

    # inicializar antes de iterar
    df_res = df_r.copy()
    dfx = df.copy()
    df_f = []
    dfx0 = pd.DataFrame()

    # columnas que se asignan mal entre iteracion y se borran
    drop_cols_ = list(df_r.columns)
    drop_cols_.append('cruce_ok')
    drop_cols_.append('secuencia_asignada')

    # rehacer proceso para criterios menos estrictos
    for i in range(5, 61, 5):
        if i != 5:
            # si no es la primera iteracion:
            df_f.append(dfx0)
            dfx = dfx1.copy()
            df_res = df_res1.copy()

        df_reservas_ok = pd.DataFrame()

        # encontrar reservas mas cercanas
        dfx = pd.merge_asof(dfx, df_res,
                            left_on='tiempo_inicial_carga',
                            right_on='fecha_hora_reserva',
                            left_by='pistola_id_stp', right_by='pistola_id_stp2',
                            suffixes=['', '_res'],
                            tolerance=timedelta(minutes=i, seconds=1),
                            direction='nearest')

        # total secuencias con carga asignada:
        sec_asignadas = len(dfx.loc[~dfx['pistola_id_stp2'].isna(), 'id_secuencia'].unique())

        # revisamos si fecha de reserva no es posterior al fin de la secuencia de carga
        dfx['cruce_ok'] = (dfx['fecha_hora_reserva'] <= dfx['tiempo_final_carga'])
        # total secuencias con carga asignada validas:
        s_asign_validas = len(dfx.loc[((~dfx['reserva_id'].isna()) & (dfx['cruce_ok'])), 'id_secuencia'].unique())

        # total secuencias con carga asignada valida pero duplicada:
        df_reservas_ok = dfx.loc[((~dfx['reserva_id'].isna()) & (dfx['cruce_ok']))].groupby(by='id_secuencia').min()
        n_res_dup = len(df_reservas_ok.index) - len(df_reservas_ok.groupby(['reserva_id']).min().index)
        logger.info(f"Merge_asof({i:02d} minutos). Secuencias con reserva valida asignada: {s_asign_validas:02d} (Duplicadas {n_res_dup:02d})")
        # en caso de asignacion duplicada quedarse con el mas cercano a tiempo de reserva
        df_reservas_ok['dif_merge'] = abs((df_reservas_ok['fecha_hora_reserva'] - df_reservas_ok['tiempo_inicial_carga']) / pd.Timedelta(minutes=1))
        df_reservas_ok.sort_values(by=['pistola_id', 'fecha_hora_evento', 'soc', 'dif_merge'], inplace=True)

        df_reservas_ok.drop_duplicates(subset='reserva_id', keep='first', inplace=True)
        id_reservas_asignadas = df_reservas_ok['reserva_id'].unique()
        id_secuencias_asignadas = df_reservas_ok.index.tolist()

        dfx['secuencia_asignada'] = dfx['id_secuencia'].isin(id_secuencias_asignadas)
        df_res['reserva_asignada'] = df_res['reserva_id'].isin(id_reservas_asignadas)

        # separar entre data lista y bien asignada y la data que aun no es bien asignada
        dfx0, dfx1 = dfx[dfx['secuencia_asignada']].copy(), dfx[~dfx['secuencia_asignada']].copy()
        df_res0, df_res1 = df_res[df_res['reserva_asignada']].copy(), df_res[~df_res['reserva_asignada']].copy()

        dfx0 = dfx0.drop(columns='secuencia_asignada')
        dfx1 = dfx1.drop(columns=drop_cols_)
        df_res0 = df_res0.drop(columns='reserva_asignada')
        df_res1 = df_res1.drop(columns='reserva_asignada')
        if dfx1.empty:
            logger.warning("Toda la data fue asignada correctamente!")
            break

    # que ultimo df se agregue al resultado final, borrando asignacion erronea de ultima iteracion
    if not dfx1.empty:
        dfx.loc[~dfx['secuencia_asignada'], drop_cols_] = pd.NA
    dfx = dfx.drop(columns='secuencia_asignada')
    df_f.append(dfx)

    df_f = pd.concat(df_f)
    df_f = df_f.sort_values(by=['pistola_id', 'fecha_hora_evento', 'soc'])
    logger.info(f"Reservas en el dia: {len(df_res.index)}")
    logger.info(f"Secuencias en el dia: {len(df['id_secuencia'].unique())}")
    if len(df_f['id_secuencia'].unique()) != len(df['id_secuencia'].unique()):
        logger.warning(f"Secuencias en el dia en data final (check): {len(df_f['id_secuencia'].unique())}")
    logger.info(f"Secuencias con reserva valida asignada: {len(df_f.loc[~df_f['reserva_id'].isna(), 'id_secuencia'].unique())}")
    if len(df_f.loc[~df_f['reserva_id'].isna(), 'reserva_id'].unique()) != len(df_f.loc[~df_f['reserva_id'].isna(), 'id_secuencia'].unique()):
        logger.warning(f"Reservas con secuencia valida asignada (check): {len(df_f.loc[~df_f['reserva_id'].isna(), 'reserva_id'].unique())}")
    logger.info(f"Datos en el dia: {len(df.index)}")
    logger.info(f"Datos en el dia con reserva valida asignada: {len(df_f.loc[~df_f['reserva_id'].isna()].index)}")

    # dejar secuencias con id unico, asume no mas de 9999 secuencias por dia
    df_f['id_secuencia'] = df_f['id_secuencia'] + 10000 * (df_f['tiempo_inicial_carga'].dt.strftime('%y%m%d').astype(int))
    return df_f


def main():
    mantener_log()
    fecha_hoy = datetime.today()
    fecha_ayer = fecha_hoy - timedelta(days=1)
    fecha_hoy = fecha_hoy.strftime('%Y-%m-%d')
    fecha_ayer = fecha_ayer.strftime('%Y-%m-%d')

    # -- Variables debug
    directorio = 'C:/Users/bstefoni/Desktop/Trabajo 2021/update_cargadores'
    fecha_hoy = '2021-09-28'
    fecha_ayer = '2021-09-27'
    do_query = False
    query_save = False
    query_load = True

    logger.info(f"Primer intento con fecha_ayer={fecha_ayer} y fecha_hoy={fecha_hoy}")

    if do_query:
        df_reserva = query_reservas_diaria(fecha_ayer, fecha_hoy)
        logger.info(f"Query reservas lista")
        df_dia = query_data_diaria(fecha_ayer, fecha_hoy)
        logger.info(f"Query data cargadores lista")
    if query_save:
        df_reserva.to_parquet('df_res.parquet', compression='gzip')
        logger.info(f"Guardado parquet reservas")
        df_dia.to_parquet('df.parquet', compression='gzip')
        logger.info(f"Guardado parquet data")
    if query_load:
        logger.info(f"Leyendo parquet reservas")
        df_reserva = pd.read_parquet(f'{directorio}/df_res.parquet')
        logger.info(f"Leyendo parquet data")
        df_dia = pd.read_parquet(f'{directorio}/df.parquet')
    logger.info(f"Lista data")

    if df_dia.empty:
        logger.warning(f"Data vacia")
    else:
        df_dia = procesar_data(df_dia, df_reserva)
        df_dia.to_parquet('df_27sept.parquet', compression='gzip')


if __name__ == '__main__':
    main()
