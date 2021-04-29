#!/usr/bin/python3
# -*- coding:utf-8 -*-
import configparser
import os
import json
import datetime

def get_file_path(file_name):
    return os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resources', file_name)
    # return os.path.join("/root/scada/starfish/config/" + file_name)


# 获取配置
def get_config():
    config = configparser.RawConfigParser()
    config.read(get_file_path("config.cfg"), encoding="utf-8")
    # config.read(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resources', 'config.cfg'), encoding="utf-8")
    return config


config = get_config()


# 时间日期格式化
def get_time_string_by_pattern(date, pattern):
    time_string = date.strftime(pattern)
    return time_string


# 获取日期
def get_time_date_by_pattern(time_string, pattern):
    date = datetime.datetime.strptime(time_string, pattern)
    return date

def get_retentionpolicy_measurements(re_me_string):
    list = re_me_string.split(".")
    return list[0], list[1]


# 获取文件夹
def get_path_dir(date, dir_path, pattern):
    time_string = get_time_string_by_pattern(date, pattern)
    path = dir_path + time_string + "/"
    if not os.path.exists(path):
        os.makedirs(path)
    return path


# 获取文件名
def get_file_name(date, pattern, model, *args):
    time_string = get_time_string_by_pattern(date, pattern)
    return model.format(time_string, *args)


# 获取最后一次写入时间
def get_last_time_config(name):
    file_read = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resources', 'lasttime.json'), mode='r',
         encoding="utf-8")
    last_dict = json.loads(file_read.read())
    file_read.close()
    return last_dict[name]

# 设置最后一次写入时间
def set_last_time(name, date):
    file_read = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resources', 'lasttime.json'), mode='r', encoding="utf-8")
    last_dict = json.loads(file_read.read())
    file_read.close()
    last_dict[name] = date
    file_write = open(os.path.join(os.path.dirname(os.path.abspath(__file__)), 'resources', 'lasttime.json'), mode='w', encoding="utf-8")
    json.dump(last_dict, file_write,ensure_ascii=False)
    file_write.close()


