import pymysql as mdb
import pytz
from time import sleep
from datetime import datetime


class SQL:
    """
    Class containing the methods that can be called to use the SQL database
    """

    @staticmethod
    def get_price_data(select, where, freq):
        cur.execute('SELECT ' + select + ' FROM ' + Utils.get_price_table(freq) + ' WHERE ' + where + ' ORDER BY ' + price_date)
        data = cur.fetchall()
        return data

    @staticmethod
    def get_exchange_name(symbol):
        cur.execute('SELECT ' + exchange_id + ' FROM ' + symbol_table + ' WHERE ' + ticker + ' = "' + symbol + '"')
        data_1 = cur.fetchone()
        result_1 = data_1[0]
        cur.execute('SELECT ' + abbrev + ' FROM ' + exchange_table + ' WHERE ' + id + ' = "' + result_1 + '"')
        data_2 = cur.fetchone()
        result_2 = data_2[0]
        return result_2

    @staticmethod
    def get_country(symbol):
        cur.execute('SELECT ' + country + ' FROM ' + symbol_table + ' WHERE ' + ticker + ' = "' + symbol + '"')
        data = cur.fetchone()
        result = data[0]
        return result

    @staticmethod
    def get_broker(country_str):
        cur.execute('SELECT ' + name + ' FROM ' + broker_table + ' WHERE ' + country + ' = "' + country_str + '"')
        data = cur.fetchone()
        result = data[0]
        return result

    @staticmethod
    def get_symbol_id(symbol):
        cur.execute('SELECT ' + id + ' FROM ' + symbol_table + ' WHERE ' + ticker + " = '" + symbol + "'")
        data = cur.fetchone()
        result = data[0]
        return result

    @staticmethod
    def get_data_vendor_id(dv_str):
        cur.execute('SELECT ' + id + ' FROM ' + data_vendor_table + ' WHERE ' + name + " = '" + dv_str + "'")
        data = cur.fetchone()
        result = data[0]
        return result

    @staticmethod
    def get_symbols_per_country(country_str):
        cur.execute('SELECT ' + ticker + ' FROM ' + symbol_table + ' WHERE ' + country + " = '" + country_str + "'")
        data = cur.fetchall()
        return data

    @staticmethod
    def insert_price_data(data, freq):
        price_table = Utils.get_price_table(freq)
        column_list = Utils.get_table_columns(price_table)
        del column_list[0]
        column_str = ', '.join(column_list)
        nb_col_str = len(column_list)
        insert_str = ("%s, " * nb_col_str)[:-2]
        final_str = "INSERT INTO " + price_table + " (%s) VALUES (%s)" % (column_str, insert_str)

        cur.executemany(final_str, data)
        con.commit()

    @staticmethod
    def insert_symbol_data(symbols, instr, country, exchange_id):
        null = 'NULL'
        for symbol in symbols:
            final_str = 'INSERT INTO ' + symbol_table + '  VALUES ' + '(' + null + ', "' + symbol + '", "' + instr + '", ' + \
                        country + ', ' + null + ', ' + null + ', ' + null + ', "' + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + '", ' + exchange_id + ');'
            cur.execute(final_str)
        con.commit()

    @staticmethod
    def remove_price_duplicates(freq):
        """
        :param freq: price frequency of the price table to be cleaned
        Remove the duplicates entries of the price table
        """
        price_table = Utils.get_price_table(freq)
        cur.execute('CREATE TABLE temp_table LIKE ' + price_table)
        sleep(0.5)
        cur.execute('ALTER TABLE temp_table ADD UNIQUE(' + price_date + ', ' + ticker + ')')
        sleep(0.5)
        cur.execute('INSERT IGNORE INTO temp_table SELECT * FROM ' + price_table)
        sleep(0.5)
        cur.execute('RENAME TABLE ' + price_table + ' TO old_' + price_table + ', temp_table TO ' + price_table)
        sleep(0.5)
        cur.execute('DROP TABLE old_' + price_table)
        sleep(0.5)
        cur.execute('ALTER TABLE ' + price_table + ' DROP INDEX ' + price_date)


class Utils:
    """
    Class containing support methods for the SQL class. These methods cannot be called directly
    """
    @staticmethod
    def get_price_table(freq):
        return freq + '_price'

    @staticmethod
    def get_table_columns(table):
        cur.execute("desc " + table)
        data = cur.fetchall()
        result = []
        for col in data:
            result.append(col[0])
        return result

    @staticmethod
    def get_mysql_connection_con():
        db_host = 'localhost'
        db_user = 'maxime'
        db_pass = '0c661A7*9'
        db_name = 'securities_master'
        con = mdb.connect(host=db_host, user=db_user, passwd=db_pass, db=db_name)
        return con

# SQL table structure variables
id = "id"
ticker = "ticker"
data_vendor_table = "data_vendor"
name = "name"
symbol_table = "symbol"
con = Utils.get_mysql_connection_con()
cur = con.cursor()
price_date = 'price_date'
timezone = pytz.utc
date_col_sql_data = 2
exchange_id = 'exchange_id'
exchange_table = 'exchange'
abbrev = 'abbrev'
open = 'open_price'
high = 'high_price'
low = 'low_price'
close = 'close_price'
adj_close = 'adj_close_price'
volume = 'volume'
country ='country'
broker_table = 'broker'
