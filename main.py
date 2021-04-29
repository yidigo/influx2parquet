#!/usr/bin/python3
# -*- coding:utf-8 -*-
from plumbum import cli
import time
import threading
import socket
from log import *
from influxdb import InfluxDBClient
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
from utils import  set_last_time,get_last_time_config
import numpy as np
from parquetcolumns import all_name_dict2
socket.setdefaulttimeout(10)


# influx -username root -password root -port 58086 -precision rfc3339


class Influxdb2parquet(cli.Application):
    # this program is build to check controller tracelog
    PROGNAME = 'data cycle Scheduler'
    VERSION = '0.1'
    config = None
    client = None
    farm = None

    def initialize(self):
        # noinspection PyBroadException
        try:
            self.client = InfluxDBClient(host=self.config.get('influxdb', 'host'),
                                         port=self.config.get('influxdb', 'port'),
                                         username=self.config.get('influxdb', 'username'),
                                         password=self.config.get('influxdb', 'password'),
                                         database=self.config.get('influxdb', 'database'))

            self.farm = get_last_time_config("farm")
            logger_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs', 'influxdb2parquet.log')
            log_init(logger_path, site_name=self.config.get("global", "site"), service='influxdb to parquet')
        except Exception as e:
            print('Start service failed.'+ str(e))
            self.client.close()
            exit(-1)
        log(level='info', msg='service init complete!')  # info

    def load_config(self):
        self.config = configparser.RawConfigParser()
        self.config.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resources', 'config.cfg'))

    def _heart_beat(self):
        while True:
            time.sleep(600)
            log('info', 'Heartbeat')

    def heart_beat(self):
        thread = threading.Thread(target=self._heart_beat, args=())
        thread.start()

    def exportInfluxdb_day(self,start_time,end_time):
        try:
            # 时间日期格式化2018-07-16T10:00:00Z 注意UTC时间比当前时间小8 小时
            start = (start_time - datetime.timedelta(hours=8))
            end = (end_time - datetime.timedelta(hours=8))
            for turbineid in range(int(self.config.get("global", "turbines"))):

                currentid = str(turbineid + 1).zfill(2) + "#"
                for measurement in eval(self.config.get("global", "measurements")):
                    pqwriter = None
                    current = start
                    while current<end:
                        query = "SELECT * FROM "+measurement+" WHERE farm = '{0}' and turbine = '{1}' and  time>= '{2}' and time < '{3}';".format(
                              self.farm, currentid, current.strftime("%Y-%m-%dT%H:%M:%SZ"), (current+datetime.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ"))
                        # print(query)
                        df = pd.DataFrame(self.client.query(query).get_points())
                        if df.size>0:
                            for c in all_name_dict2:
                                try:
                                    df[c]
                                except:
                                    df[c]=np.nan
                            df = df.reindex(columns=all_name_dict2)
                            df =df.fillna(np.nan)
                            df['time'] = pd.to_datetime(df['time']).dt.tz_localize(None)
                            df['time'] = df['time'] + datetime.timedelta(hours=8)
                            dir_name = self.config.get("global", "uploadpath") + start_time.strftime("%Y-%m-%d") +"/"
                            filename = currentid+ "_" + start_time.strftime("%Y-%m-%d") + "_" + end_time.strftime("%Y-%m-%d") + '.parquet'
                            filepath = dir_name + filename
                            if not os.path.exists(dir_name):
                                os.makedirs(dir_name)
                            table = pa.Table.from_pandas(df)
                            if pqwriter is None:
                                pqwriter = pq.ParquetWriter(filepath, table.schema)
                            pqwriter.write_table(table=table)

                            print("write parquet ["+filepath+"]")
                            log("info", "export " + measurement + " to " + filepath + " success")
                        current=current + datetime.timedelta(hours=1)
                    if pqwriter:
                        pqwriter.close()
            time_string = end_time.strftime("%Y-%m-%d")
            set_last_time("influxdb_day_lasttime", time_string)
            log("info", "end influx day data , date is: " + str(start_time) + " - " + str(end_time))
        except Exception as e:
            print(e)
            log("error", str(e))

    # 从last_day递增 一天 直到now-1
    def data_complement(self):
        try:
            # influxdb每日导出执行日期
            influxdb_day_time_string = get_last_time_config("influxdb_day_lasttime")
            print(influxdb_day_time_string+"start.")
            last_infuxdb_day = datetime.datetime.strptime(influxdb_day_time_string, "%Y-%m-%d")
            influx_different_days = (datetime.datetime.now() - last_infuxdb_day).days
            end_influx_day_date = datetime.datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            if influx_different_days > 0:#导出历史文件
                log("info", "start influx day data [" +influxdb_day_time_string+"]---["+ str(influx_different_days) + "] day")
                for day in range(influx_different_days):
                    start = end_influx_day_date - datetime.timedelta(days=influx_different_days - day)
                    end = end_influx_day_date - datetime.timedelta(days=influx_different_days - day - 1)
                    #导出文件
                    self.exportInfluxdb_day(start,end)
            elif influx_different_days == 0: #导出当天文件
                log("info", "start influx day data [" +influxdb_day_time_string+"]---["+ str(influx_different_days) + "] day")
                start = end_influx_day_date
                end = end_influx_day_date + datetime.timedelta(days=1)
                #导出当天文件
                self.exportInfluxdb_day(start,end)

        except Exception as e:
            log("error", "influx day data complement failed, " + str(e))


    def main(self):
        print("-----------")
        self.load_config()
        self.initialize()
        # check time every 10 minutes 检测心跳
        self.heart_beat()
        while True:
            # 从 lasttime  到 now 每一天导出一个文件
            self.data_complement()
            print(datetime.datetime.now().strftime("%Y-%m-%d")+"finished.")
            # 每隔 1小时检测一次
            time.sleep(3600)


if __name__ == '__main__':
    Influxdb2parquet.run()

