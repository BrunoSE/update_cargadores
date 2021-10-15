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


def query_0(query_string):
    # entrega tabla de tipo de dia segun fecha, a partir de fecha_str en formato %Y-m-d
    db0 = MySQLdb.connect(host="192.168.11.150",
                          user="brunom",
                          passwd="Manzana",
                          db="stp_estacionamiento")

    cur0 = db0.cursor()

    query0 = f"SELECT * FROM pistolas;"
    cur0.execute(query0)
    df_ = pd.DataFrame([row for row in cur0.fetchall() if row[0] is not None],
                       columns=[i[0] for i in cur0.description])

    cur0.close()
    db0.close()

    return df_


def main():
    mantener_log()
    logger.info("Primer intento")
    df = query_0('')
    df.to_excel('df.xlsx', index=True)


if __name__ == '__main__':
    main()
