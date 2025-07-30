import pandas as pd
import numpy as np
import os
import shutil
import warnings
import json
import pymysql
import subprocess
import sys
from datetime import time, datetime, timedelta, date
# 导入全局配置
from global_dic import get as glv
from utils import data_getting_glb,source_getting
global df_date,source


def Chinese_valuation_date():
    """
    获取中国交易日期数据

    Returns:
        pandas.DataFrame: 交易日期数据
    """
    try:
        inputpath = glv('valuation_date')
        df_date = data_getting_glb(inputpath)
        if df_date.empty:
            return pd.DataFrame(columns=['valuation_date'])

        if 'valuation_date' in df_date.columns:
            df_date['valuation_date'] = df_date['valuation_date'].str.strip()
            return df_date
        else:
            return pd.DataFrame(columns=['valuation_date'])
    except Exception as e:
        return pd.DataFrame(columns=['valuation_date'])


# 初始化全局变量
df_date = Chinese_valuation_date()
source=source_getting()

def next_workday_auto():
    """
    获取下一个工作日

    Returns:
        str: 下一个工作日
    """
    today = date.today()
    today = today.strftime('%Y-%m-%d')
    try:
        if df_date.empty:
            return today
        index_today = df_date[df_date['valuation_date'] == today].index.tolist()[0]
        index_tommorow = index_today + 1
        tommorow = df_date.iloc[index_tommorow].tolist()[0]
    except:
        if df_date.empty:
            return today
        index_today = df_date[df_date['valuation_date'] >= today].index.tolist()[0]
        tommorow = df_date.iloc[index_today].tolist()[0]
    return tommorow


def last_workday_auto():
    """
    获取上一个工作日

    Returns:
        str: 上一个工作日
    """
    today = date.today()
    today = today.strftime('%Y-%m-%d')
    try:
        if df_date.empty:
            return today
        index_today = df_date[df_date['valuation_date'] == today].index.tolist()[0]
        index_tommorow = index_today - 1
        yesterday = df_date.iloc[index_tommorow].tolist()[0]
    except:
        if df_date.empty:
            return today
        index_today = df_date[df_date['valuation_date'] <= today].index.tolist()[-1]
        yesterday = df_date.iloc[index_today].tolist()[0]
    return yesterday


def last_workday_calculate(x):
    """
    计算指定日期的上一个工作日

    Args:
        x (str/datetime): 指定日期

    Returns:
        str: 上一个工作日
    """
    today = x
    try:
        today = today.strftime('%Y-%m-%d')
    except:
        today = today
    if df_date.empty:
        print("警告: 未找到交易日期数据")
        return today
    yesterday = df_date[df_date['valuation_date'] < today]['valuation_date'].tolist()[-1]
    return yesterday


def next_workday_calculate(x):
    """
    计算指定日期的下一个工作日

    Args:
        x (str/datetime): 指定日期

    Returns:
        str: 下一个工作日
    """
    today = x
    try:
        today = today.strftime('%Y-%m-%d')
    except:
        today = today
    if df_date.empty:
        print("警告: 未找到交易日期数据")
        return today
    try:
        index_today = df_date[df_date['valuation_date'] == today].index.tolist()[0]
        index_tommorow = index_today + 1
        tommorow = df_date.iloc[index_tommorow].tolist()[0]
    except:
        index_today = df_date[df_date['valuation_date'] >= today].index.tolist()[0]
        tommorow = df_date.iloc[index_today].tolist()[0]
    return tommorow


def last_workday_calculate2(df_score):
    """
    批量计算上一个工作日

    Args:
        df_score (pandas.DataFrame): 包含date列的数据框

    Returns:
        pandas.DataFrame: 处理后的数据框
    """
    if df_date.empty:
        print("警告: 未找到交易日期数据")
        return df_score
    df_final = pd.DataFrame()
    date_list = df_score['date'].unique().tolist()
    for date in date_list:
        slice_df = df_score[df_score['date'] == date]
        yesterday = last_workday_calculate(date)
        slice_df['date'] = yesterday
        df_final = pd.concat([df_final, slice_df])
    return df_final


def is_workday(today):
    """
    判断是否为工作日

    Args:
        today (str): 日期

    Returns:
        bool: 是否为工作日
    """
    try:
        df_date2 = df_date[df_date['valuation_date'] == today]
    except:
        df_date2 = pd.DataFrame()
    if len(df_date2) != 1:
        return False
    else:
        return True


def working_days(df):
    """
    筛选工作日数据

    Args:
        df (pandas.DataFrame): 包含date列的数据框

    Returns:
        pandas.DataFrame: 处理后的数据框
    """
    date_list = df_date['valuation_date'].tolist()
    df = df[df['date'].isin(date_list)]
    return df


def is_workday_auto():
    """
    判断今天是否为工作日

    Returns:
        bool: 是否为工作日
    """
    today = date.today()
    today = today.strftime('%Y-%m-%d')
    try:
        df_date2 = df_date[df_date['valuation_date'] == today]
    except:
        df_date2 = pd.DataFrame()
    if len(df_date2) != 1:
        return False
    else:
        return True


def intdate_transfer(date):
    """
    日期转整数格式

    Args:
        date (str/datetime): 日期

    Returns:
        str: 整数格式日期
    """
    date = pd.to_datetime(date)
    date = date.strftime('%Y%m%d')
    return date


def strdate_transfer(date):
    """
    日期转字符串格式

    Args:
        date (str/datetime): 日期

    Returns:
        str: 字符串格式日期
    """
    date = pd.to_datetime(date)
    date = date.strftime('%Y-%m-%d')
    return date


def working_days_list(start_date, end_date):
    """
    获取工作日列表

    Args:
        start_date (str): 开始日期
        end_date (str): 结束日期

    Returns:
        list: 工作日列表
    """
    global df_date
    df_date_copy = df_date.copy()
    df_date_copy.rename(columns={'valuation_date': 'date'}, inplace=True)
    df_date_copy = df_date_copy[(df_date_copy['date'] >= '2010-12-31') &
                                (df_date_copy['date'] <= '2030-01-01')]
    df_date_copy['target_date'] = df_date_copy['date']
    df_date_copy.dropna(inplace=True)
    df_date_copy = df_date_copy[(df_date_copy['date'] >= start_date) &
                                (df_date_copy['date'] <= end_date)]
    date_list = df_date_copy['target_date'].tolist()
    return date_list


def working_day_count(start_date, end_date):
    """
    计算工作日天数

    Args:
        start_date (str): 开始日期
        end_date (str): 结束日期

    Returns:
        int: 工作日天数
    """
    global df_date
    slice_df_date = df_date[df_date['valuation_date'] > start_date]
    slice_df_date = slice_df_date[slice_df_date['valuation_date'] <= end_date]
    total_day = len(slice_df_date)
    return total_day


def month_lastday_df():
    """
    获取每月最后工作日

    Returns:
        list: 每月最后工作日列表
    """
    df_date['year_month'] = df_date['valuation_date'].apply(lambda x: str(x)[:7])
    month_lastday = df_date.groupby('year_month')['valuation_date'].tail(1).tolist()
    return month_lastday


def last_weeks_lastday_df():
    """
    获取上周最后工作日

    Returns:
        str: 上周最后工作日
    """
    today = date.today()
    today = today.strftime('%Y-%m-%d')
    inputpath = glv('weeks_lastday')
    df_lastday = data_getting_glb(inputpath)
    if source == 'sql':
        df_lastday = df_lastday[df_lastday['type'] == 'weeksLastDay']
    if df_lastday.empty:
        print("警告: 未找到周最后工作日数据")
        return today
    lastday = df_lastday[df_lastday['valuation_date'] < today]['valuation_date'].tolist()[-1]
    return lastday


def last_weeks_lastday(date):
    """
    获取指定日期的上周最后工作日

    Args:
        date (str): 指定日期

    Returns:
        str: 上周最后工作日
    """
    inputpath = glv('weeks_lastday')
    df_lastday = data_getting_glb(inputpath)
    if source == 'sql':
        df_lastday = df_lastday[df_lastday['type'] == 'weeksLastDay']
    if df_lastday.empty:
        print("警告: 未找到周最后工作日数据")
        return date
    date = pd.to_datetime(date)
    date = date.strftime('%Y-%m-%d')
    lastday = df_lastday[df_lastday['valuation_date'] < date]['valuation_date'].tolist()[-1]
    return lastday


def weeks_firstday(date):
    """
    获取周第一个工作日

    Args:
        date (str): 日期

    Returns:
        str: 周第一个工作日
    """
    inputpath = glv('weeks_firstday')
    df_firstday = data_getting_glb(inputpath)
    if source == 'sql':
        df_firstday = df_firstday[df_firstday['type'] == 'weeksFirstDay']
    if df_firstday.empty:
        print("警告: 未找到周第一个工作日数据")
        return date
    firstday = df_firstday[df_firstday['valuation_date'] < date]['valuation_date'].tolist()[-1]
    return firstday


def next_weeks_lastday(date):
    """
    获取下周最后工作日

    Args:
        date (str): 日期

    Returns:
        str: 下周最后工作日
    """
    date = pd.to_datetime(date)
    date = date.strftime('%Y-%m-%d')
    inputpath = glv('weeks_lastday')
    df_lastday = data_getting_glb(inputpath)
    if source == 'sql':
        df_lastday = df_lastday[df_lastday['type'] == 'weeksLastDay']
    if df_lastday.empty:
        print("警告: 未找到周最后工作日数据")
        return date
    lastday = df_lastday[df_lastday['valuation_date'] > date]['valuation_date'].tolist()[0]
    return lastday