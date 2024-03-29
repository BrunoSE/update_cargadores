#!/usr/bin/env python
# coding: utf-8
import pandas as pd
import numpy as np
import MySQLdb
import logging
from sqlalchemy import create_engine
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


def query_data_diaria(fecha_str_ayer, fecha_str_hoy, tabla_filtrada=False):
    # Query entrega 24 horas de data telemetria de cargadores a partir de las 7:00pm de un dia
    tabla = "cargadores_historico"
    if tabla_filtrada:
        tabla = "cargadores_historico_filtrado"

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
                        stp_estacionamiento.{tabla}
                    WHERE
                        power_active_import > '20' AND
                        voltage <> '0' AND
                        current_import <> '0' AND
                        soc <= '99' AND
                        fecha_hora_evento BETWEEN '{fecha_str_ayer} 19:00:01' AND '{fecha_str_hoy} 19:00:00'
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
                    fecha_hora_reserva BETWEEN '{fecha_str_ayer} 19:00:01' AND '{fecha_str_hoy} 19:00:00';
                """
             )

    cur0.execute(query0)
    df0_ = pd.DataFrame([row for row in cur0.fetchall() if row[0] is not None],
                        columns=[i[0] for i in cur0.description])

    cur0.close()
    db0.close()

    return df0_


def procesar_data(df, df_r, fecha_hoy_, columna_fechahora='fecha_hora_consulta'):
    #  columna_fechahora = 'fecha_hora_evento'  # cambiar si se quiere ocupar fh evento de copec

    # asegurar id sea it, ordenar data antes de hacer analisis de secuencias
    if not df_r.empty:
        df_r['reserva_id'] = df_r['reserva_id'].astype(int)
    else:
        logger.warning("Data reservas vacia, se procesara tabla de todas formas sin esta data")

    df = df.sort_values(by=['pistola_id', columna_fechahora, 'soc'])

    # definir secuencias
    df['d_soc'] = df['soc'].shift(-1) - df['soc']
    df['dT'] = (df[columna_fechahora].shift(-1) - df[columna_fechahora]) / pd.Timedelta(minutes=1)
    df['fin_secuencia'] = (((df['dT'] > 20) | (df['d_soc'] > 10)) | ((df['d_soc'] < 0) | (df['pistola_id'].shift(-1) != df['pistola_id'])))

    # asignar id unico a cada secuencia del dia
    df['inicio_secuencia'] = df['fin_secuencia'].shift(1)
    # primer valor de esta columna es NA, se reemplaza por True
    df['inicio_secuencia'].iloc[0:1].fillna(value=True, inplace=True)
    df['id_secuencia'] = df['inicio_secuencia'].astype(int).cumsum(axis=0)

    # asignar tiempo inicial y final a cada secuencia
    dfg_ini = df[['id_secuencia', columna_fechahora]].groupby(by='id_secuencia').min()
    dfg_fin = df[['id_secuencia', columna_fechahora]].groupby(by='id_secuencia').max()

    dfg_ini.rename(columns={columna_fechahora: 'tiempo_inicial_carga'}, inplace=True)
    dfg_fin.rename(columns={columna_fechahora: 'tiempo_final_carga'}, inplace=True)

    dfg = dfg_ini.merge(dfg_fin, how='outer', left_index=True, right_index=True)
    df = df.merge(dfg, how='left', left_on='id_secuencia', right_index=True)

    # antes de merge_asof se requiere ordenar data en las llaves del join
    df.sort_values(by=['tiempo_inicial_carga'], inplace=True)

    if not df_r.empty:
        # si data reserva no esta vacia se procede a hacer cruce
        df_r.sort_values(by=['fecha_hora_reserva'], inplace=True)

        # columnas que se asignan mal entre iteracion y se borran
        drop_cols_ = list(df_r.columns)
        drop_cols_.append('cruce_ok')
        drop_cols_.append('secuencia_asignada')

        # inicializar antes de iterar
        df_res = df_r.copy()
        dfx = df.copy()
        df_f = []
        dfx0 = pd.DataFrame()
        dfx1 = pd.DataFrame()
        df_res1 = pd.DataFrame()

        # rehacer proceso para criterios cada vez menos estrictos
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
                                allow_exact_matches=True,
                                direction='backward')

            # revisamos si fecha de reserva no es posterior al fin de la secuencia de carga
            dfx['cruce_ok'] = (dfx['fecha_hora_reserva'] <= dfx['tiempo_final_carga'])
            # total secuencias con carga asignada validas:
            s_asign_validas = len(dfx.loc[((~dfx['reserva_id'].isna()) & (dfx['cruce_ok'])), 'id_secuencia'].unique())

            # total secuencias con carga asignada valida pero duplicada:
            df_reservas_ok = dfx.loc[((~dfx['reserva_id'].isna()) & (dfx['cruce_ok']))].groupby(by='id_secuencia').min()
            n_res_dup = len(df_reservas_ok.index) - len(df_reservas_ok.groupby(['reserva_id']).min().index)
            logger.debug(f"Merge_asof({i:02d} minutos). Secuencias con reserva valida asignada: {s_asign_validas:02d} (Duplicadas {n_res_dup:02d})")
            # en caso de asignacion duplicada quedarse con el mas cercano a tiempo de reserva
            df_reservas_ok['dif_merge'] = abs((df_reservas_ok['fecha_hora_reserva'] - df_reservas_ok['tiempo_inicial_carga']) / pd.Timedelta(minutes=1))
            df_reservas_ok.sort_values(by=['pistola_id', columna_fechahora, 'soc', 'dif_merge'], inplace=True)

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
    else:
        # si data reservas esta vacia, solo copiar la data y agregar columnas vacias
        df_f = df.copy()
        df_f[list(df_r.columns)] = pd.NA
        df_f['cruce_ok'] = pd.NA

    df_f = df_f.sort_values(by=['pistola_id', columna_fechahora, 'soc'])


    # --- Printear logs

    hora_limite_turno = f"{fecha_hoy_} 08:00:00"
    logger.info(f"Total Reservas: {len(df_r.index)}")

    logger.info(f"Total Secuencias: {len(df['id_secuencia'].unique())}")
    if len(df_f['id_secuencia'].unique()) != len(df['id_secuencia'].unique()):
        logger.warning(f"Total Secuencias (check): {len(df_f['id_secuencia'].unique())}")

    logger.info(f"Total Secuencias con reserva valida asignada: {len(df_f.loc[~df_f['reserva_id'].isna(), 'id_secuencia'].unique())}")
    if len(df_f.loc[~df_f['reserva_id'].isna(), 'reserva_id'].unique()) != len(df_f.loc[~df_f['reserva_id'].isna(), 'id_secuencia'].unique()):
        logger.warning(f"Total Reservas con secuencia valida asignada (check): {len(df_f.loc[~df_f['reserva_id'].isna(), 'reserva_id'].unique())}")

    df_r_dia = df_r.loc[df_r['fecha_hora_reserva'] >= pd.Timestamp(hora_limite_turno)]
    df_r_noche = df_r.loc[df_r['fecha_hora_reserva'] < pd.Timestamp(hora_limite_turno)]

    df_dia = df.loc[df[columna_fechahora] >= pd.Timestamp(hora_limite_turno)]
    df_noche = df.loc[df[columna_fechahora] < pd.Timestamp(hora_limite_turno)]

    df_f_dia = df_f.loc[df_f[columna_fechahora] >= pd.Timestamp(hora_limite_turno)]
    df_f_noche = df_f.loc[df_f[columna_fechahora] < pd.Timestamp(hora_limite_turno)]

    logger.info(f"Total Reservas (turno dia): {len(df_r_dia.index)}")
    logger.info(f"Total Secuencias (turno dia): {len(df_dia['id_secuencia'].unique())}")
    if len(df_f_dia['id_secuencia'].unique()) != len(df_dia['id_secuencia'].unique()):
        logger.warning(f"Total Secuencias (turno dia) (check): {len(df_f_dia['id_secuencia'].unique())}")

    logger.info(f"Total Secuencias con reserva valida asignada (turno dia): {len(df_f_dia.loc[~df_f_dia['reserva_id'].isna(), 'id_secuencia'].unique())}")
    if len(df_f_dia.loc[~df_f_dia['reserva_id'].isna(), 'reserva_id'].unique()) != len(df_f_dia.loc[~df_f_dia['reserva_id'].isna(), 'id_secuencia'].unique()):
        logger.warning(f"Total Reservas con secuencia valida asignada (turno dia) (check): {len(df_f_dia.loc[~df_f_dia['reserva_id'].isna(), 'reserva_id'].unique())}")

    logger.info(f"Total Reservas (turno noche): {len(df_r_noche.index)}")
    logger.info(f"Total Secuencias (turno noche): {len(df_noche['id_secuencia'].unique())}")
    if len(df_f_noche['id_secuencia'].unique()) != len(df_noche['id_secuencia'].unique()):
        logger.warning(f"Total Secuencias (turno noche) (check): {len(df_f_noche['id_secuencia'].unique())}")

    logger.info(f"Total Secuencias con reserva valida asignada (turno noche): {len(df_f_noche.loc[~df_f_noche['reserva_id'].isna(), 'id_secuencia'].unique())}")
    if len(df_f_noche.loc[~df_f_noche['reserva_id'].isna(), 'reserva_id'].unique()) != len(df_f_noche.loc[~df_f_noche['reserva_id'].isna(), 'id_secuencia'].unique()):
        logger.warning(f"Total Reservas con secuencia valida asignada (turno noche) (check): {len(df_f_noche.loc[~df_f_noche['reserva_id'].isna(), 'reserva_id'].unique())}")

    logger.info(f"Total Datos en el dia: {len(df.index)}")
    logger.info(f"Total Datos en el dia con reserva valida asignada: {len(df_f.loc[~df_f['reserva_id'].isna()].index)}")

    # --- Fin Printear logs

    n_val_dia = len(df_f.loc[~df_f['reserva_id'].isna(), 'id_secuencia'].unique())
    if n_val_dia != 0:
        n_val_dia = round(len(df_f.loc[~df_f['reserva_id'].isna()].index) / n_val_dia, 1)
        logger.info(f"Datos promedio en cada secuencia asignada valida: {n_val_dia}")
    # dejar secuencias con id unico, asume no mas de 9999 secuencias por dia
    df_f['id_secuencia'] = df_f['id_secuencia'] + 10000 * int(fecha_hoy_[2:].replace('-', ''))
    df_f.set_index('id', drop=True, append=False, inplace=True)

    metadata = []
    metadata.append(fecha_hoy_)
    metadata.append(len(df_r.index))
    metadata.append(len(df['id_secuencia'].unique()))
    metadata.append(len(df_f.loc[~df_f['reserva_id'].isna(), 'id_secuencia'].unique()))

    metadata.append(len(df_r_dia.index))
    metadata.append(len(df_dia['id_secuencia'].unique()))
    metadata.append(len(df_f_dia.loc[~df_f_dia['reserva_id'].isna(), 'id_secuencia'].unique()))

    metadata.append(len(df_r_noche.index))
    metadata.append(len(df_noche['id_secuencia'].unique()))
    metadata.append(len(df_f_noche.loc[~df_f_noche['reserva_id'].isna(), 'id_secuencia'].unique()))

    metadata.append(len(df.index))
    metadata.append(len(df_f.loc[~df_f['reserva_id'].isna()].index))

    metadata.append(n_val_dia)
    df_f = df_f.loc[df_f['cruce_ok'] == True]
    return df_f, metadata


def cargar_SQL_metadata(metadata):
    columnas_metadata = ['fecha', 'Total_Reservas', 'Total_Secuencias', 'Total_Secuencias_Asignadas',
                         'Total_Reservas_Dia', 'Total_Secuencias_Dia', 'Total_Secuencias_Asignadas_Dia',
                         'Total_Reservas_Noche', 'Total_Secuencias_Noche', 'Total_Secuencias_Asignadas_Noche', 
                         'Total_Datos', 'Total_Datos_Asignados', 'Datos_Promedio_Secuencia']

    if not metadata:
        logger.warning(f"Metadata vacia, no se carga en SQL")
        return None

    df_metadata = pd.DataFrame([metadata], columns=columnas_metadata)

    nombre_tabla_sql = 'metadata_cargadores_procesado'

    logger.info(f"Insertando data en tabla SQL: {nombre_tabla_sql}")
    # Credentials to database connection
    hostname = "192.168.11.150"
    dbname = "stp_estacionamiento"
    uname = "brunom"
    pwd = "Manzana"

    # Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                        .format(host=hostname, db=dbname, user=uname, pw=pwd))

    # Convert dataframe to sql table                                   
    df_metadata.to_sql(nombre_tabla_sql, engine, index=False, if_exists='append')
    logger.info(f"Data agregada exitosamente a tabla SQL: {nombre_tabla_sql}")
    return None


def cargar_SQL(df_sql):
    if df_sql.empty:
        logger.warning(f"Data procesada vacia, no se carga en SQL")
        return None

    nombre_tabla_sql = 'cargadores_procesado'

    logger.info(f"Insertando data en tabla SQL: {nombre_tabla_sql}")
    # Credentials to database connection
    hostname = "192.168.11.150"
    dbname = "stp_estacionamiento"
    uname = "brunom"
    pwd = "Manzana"

    # Create SQLAlchemy engine to connect to MySQL Database
    engine = create_engine("mysql+pymysql://{user}:{pw}@{host}/{db}"
                        .format(host=hostname, db=dbname, user=uname, pw=pwd))

    # Convert dataframe to sql table                                   
    df_sql.to_sql(nombre_tabla_sql, engine, index=True, if_exists='append')
    logger.info(f"Data agregada exitosamente a tabla SQL: {nombre_tabla_sql}")


def main():
    #  tabla filtrada tiene fechas entre 20 abril 2021 y 26 sept 2021, entonces:
    #  usar fecha_evento con tabla filtrada entre 2021-04-20 hasta 2021-09-04
    #  usar fecha_consulta con tabla filtrada entre 2021-09-05 y 2021-09-26
    #  usar fecha_consulta con tabla normal hasta ayer
    #  dejar corriendo con fecha_consulta diariamente
    mantener_log()

    fechas_manual = False
    fechas_historicas = False

    if fechas_manual:
        # Caso proceso manual: definir variables debug
        directorio = 'C:/Users/bruno/Desktop/Trabajo 2021/update_cargadores'
        fecha_hoy = '2021-04-27'
        fecha_ayer = '2021-04-26'
        do_query = True
        query_save = True
        query_load = False

        logger.info(f"Modo manual con fecha {fecha_hoy}")

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
        logger.info(f"Lista data manual, procesando:")
        if df_dia.empty:
            logger.warning(f"Data vacia, proceso terminado anticipadamente")
        else:
            df_dia, metadata = procesar_data(df_dia, df_reserva, fecha_hoy)
            # cargar_SQL(df_dia)
            # df_dia.to_parquet('df.parquet', compression='gzip')
            logger.info(f"Modo manual termino exitosamente")

    elif fechas_historicas:
        fecha_hoy = '2021-09-04'  # empieza desde esta fecha
        fecha_fin = '2021-09-04'  # no hace esta ultima fecha

        # Inicializar fecha_hoy
        fecha_ayer = (datetime.strptime(fecha_hoy, '%Y-%m-%d') + timedelta(days=-1)).strftime('%Y-%m-%d')
        # Guardar log en archivo
        file_handler = logging.FileHandler(f"logs/Hist_{fecha_ayer[:-3].replace('-', '_')}_{fecha_fin[:-3].replace('-', '_')}.log")
        file_handler.setLevel(logging.INFO)  # no deja pasar los debug, solo info hasta critical
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)

        logger.info(f"Empezando modo historico entre {fecha_hoy} y {fecha_fin}")
        # loop por seguridad asumimos menos de un anno de data historica para evitar looplock
        for i in range(365):
            if fecha_hoy == fecha_fin:
                logger.info(f"Calculo finalizado")
                break

            logger.info(f"Procesando fecha historica: {fecha_hoy}")
            df_reserva = query_reservas_diaria(fecha_ayer, fecha_hoy)
            df_dia = query_data_diaria(fecha_ayer, fecha_hoy, tabla_filtrada=False)
            logger.info(f"Query realizada, procesando..")
            if df_dia.empty:
                logger.warning(f"Data vacia, se procede a siguiente fecha")
            else:
                df_dia, metadata = procesar_data(df_dia, df_reserva, fecha_hoy, columna_fechahora='fecha_hora_consulta')
                cargar_SQL(df_dia)
                cargar_SQL_metadata(metadata)

            # redefinir fechas para siguiente iteracion
            fecha_ayer = (datetime.strptime(fecha_ayer, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
            fecha_hoy = (datetime.strptime(fecha_hoy, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

        logger.info(f"Modo historico con fecha {fecha_hoy} termino exitosamente")

    else:
        # definir fechas hoy y ayer
        fecha_hoy = datetime.today()
        fecha_ayer = fecha_hoy - timedelta(days=1)
        fecha_hoy = fecha_hoy.strftime('%Y-%m-%d')
        fecha_ayer = fecha_ayer.strftime('%Y-%m-%d')

        # Guardar log en archivo
        file_handler = logging.FileHandler(f"/home/apple/Documentos/update_cargadores/logs/{fecha_ayer[:-3].replace('-', '_')}.log")
        file_handler.setLevel(logging.INFO)  # no deja pasar los debug, solo info hasta critical
        file_handler.setFormatter(file_format)
        logger.addHandler(file_handler)

        # Modo automatico:
        logger.info(f"Modo automatico con fecha {fecha_hoy}")

        df_reserva = query_reservas_diaria(fecha_ayer, fecha_hoy)
        logger.info(f"Query reservas lista")
        df_dia = query_data_diaria(fecha_ayer, fecha_hoy)
        logger.info(f"Query data cargadores lista")
        if df_dia.empty:
            logger.warning(f"Data vacia, proceso finalizado anticipadamente")
        else:
            df_dia, metadata = procesar_data(df_dia, df_reserva, fecha_hoy)
            cargar_SQL(df_dia)
            cargar_SQL_metadata(metadata)

        logger.info(f"Modo automatico con fecha {fecha_hoy} termino exitosamente")


if __name__ == '__main__':
    main()
