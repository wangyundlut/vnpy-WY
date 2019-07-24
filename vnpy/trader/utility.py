"""
General utility functions.
"""

import json
import re
from pathlib import Path
from typing import Callable
from datetime import datetime, timedelta

import numpy as np
import talib
import math

from .object import BarData, TickData
from .constant import Exchange, Interval

####
# 增
# 晚稻 鸡蛋 硅铁 锰硅 苹果 红枣
DAYGROUP_1500 = ['wr', 'jd', 'sf', 'sm', 'ap', 'cj']
# 塑料 PVC EG PP
# 螺纹 热卷 燃油 沥青 橡胶 纸浆
# 豆粕 豆油 豆一 豆二 棕榈油 玉米 玉米淀粉
# 焦炭 焦煤 铁矿
NIGHTGROUP_2300 = ['l', 'v', 'eg', 'pp',
                   'rb', 'hc', 'fu', 'bu', 'ru', 'sp',
                   'm', 'y', 'a', 'b', 'p', 'c', 'cs',
                   'j', 'jm', 'i']
# 白糖 棉花 棉纱 动力煤 玻璃 PTA 甲醇 菜油 菜粕
NIGHTGROUP_2330 = ['sr', 'cf', 'cy', 'zc', 'fg', 'ta', 'ma', 'oi', 'rm']
# 铜 铝 锌 铅 镍 锡
NIGHTGROUP_0100 = ['cu', 'al', 'zn', 'pb', 'ni', 'sn']
# 原油 黄金 白银
NIGHTGROUP_0230 = ['sc', 'au', 'ag']
COMMODITY = ['wr', 'jd', 'sf', 'sm', 'ap', 'cj',
             'l', 'v', 'eg', 'pp',
             'rb', 'hc', 'fu', 'bu', 'ru', 'sp',
             'm', 'y', 'a', 'b', 'p', 'c', 'cs',
             'j', 'jm', 'i',
             'sr', 'cf', 'cy', 'zc', 'fg', 'ta', 'ma', 'oi', 'rm',
             'cu', 'al', 'zn', 'pb', 'ni', 'sn',
             'sc', 'au', 'ag'
             ]
FINANCE = ["if", "ih", "ic", "t", "ts", "tf"]


def extract_vt_symbol(vt_symbol: str):
    """
    :return: (symbol, exchange)
    """
    symbol, exchange_str = vt_symbol.split(".")
    return symbol, Exchange(exchange_str)


def generate_vt_symbol(symbol: str, exchange: Exchange):
    return f"{symbol}.{exchange.value}"


def _get_trader_dir(temp_name: str):
    """
    Get path where trader is running in.
    """
    cwd = Path.cwd()
    temp_path = cwd.joinpath(temp_name)

    # If .vntrader folder exists in current working directory,
    # then use it as trader running path.
    if temp_path.exists():
        return cwd, temp_path

    # Otherwise use home path of system.
    home_path = Path.home()
    temp_path = home_path.joinpath(temp_name)

    # Create .vntrader folder under home path if not exist.
    if not temp_path.exists():
        temp_path.mkdir()

    return home_path, temp_path


TRADER_DIR, TEMP_DIR = _get_trader_dir(".vntrader")


def get_file_path(filename: str):
    """
    Get path for temp file with filename.
    """
    return TEMP_DIR.joinpath(filename)


def get_folder_path(folder_name: str):
    """
    Get path for temp folder with folder name.
    """
    folder_path = TEMP_DIR.joinpath(folder_name)
    if not folder_path.exists():
        folder_path.mkdir()
    return folder_path


def get_icon_path(filepath: str, ico_name: str):
    """
    Get path for icon file with ico name.
    """
    ui_path = Path(filepath).parent
    icon_path = ui_path.joinpath("ico", ico_name)
    return str(icon_path)


def load_json(filename: str):
    """
    Load data from json file in temp path.
    """
    filepath = get_file_path(filename)

    if filepath.exists():
        with open(filepath, mode="r", encoding="UTF-8") as f:
            data = json.load(f)
        return data
    else:
        save_json(filename, {})
        return {}


def save_json(filename: str, data: dict):
    """
    Save data into json file in temp path.
    """
    filepath = get_file_path(filename)
    with open(filepath, mode="w+", encoding="UTF-8") as f:
        json.dump(
            data,
            f,
            indent=4,
            ensure_ascii=False
        )


def round_to(value: float, target: float):
    """
    Round price to price tick value.
    """
    rounded = int(round(value / target)) * target
    return rounded


class BarGenerator:
    """
    For: 
    1. generating 1 minute bar data from tick data
    2. generateing x minute bar/x hour bar data from 1 minute data

    Notice:
    1. for x minute bar, x must be able to divide 60: 2, 3, 5, 6, 10, 15, 20, 30
    2. for x hour bar, x can be any number
    """

    def __init__(
        self,
        on_bar: Callable,
        window: int = 0,
        on_window_bar: Callable = None,
        interval: Interval = Interval.MINUTE
    ):
        """Constructor"""
        self.bar = None
        self.on_bar = on_bar

        self.interval = interval
        self.interval_count = 0

        self.window = window
        self.window_bar = None
        self.on_window_bar = on_window_bar

        self.last_tick = None
        self.last_bar = None

        self.open_flag = False

    def update_tick(self, tick: TickData):
        """
        Update new tick data into generator.
        """
        new_minute = False

        # Filter tick data with 0 last price
        if not tick.last_price:
            return

        if not self.bar:
            new_minute = True
        # 这里修改成,如果tick的时间是下一分钟了,就推送原来的BarData 或者 tick的时间大于等于Bar的datetime end
        # 原逻辑有问题,因为Bar是闭开的,不是开闭区间
        elif (self.bar.datetime.minute != tick.datetime.minute) or \
                (tick.datetime >= self.bar.datetime_end):
            self.on_bar(self.bar)

            new_minute = True

        if new_minute:

            symb = re.sub(r'\d', '', tick.symbol).lower()
            if symb in COMMODITY:
                # 如果是早上刚开盘9:00 或者10:30 13:30 21:00,不允许修改开盘价
                if tick.datetime.hour == 9 and tick.datetime.minute == 0:
                    self.open_flag = False
                elif tick.datetime.hour == 10 and tick.datetime.minute == 30:
                    self.open_flag = False
                elif tick.datetime.hour == 13 and tick.datetime.minute == 30:
                    self.open_flag = False
                elif tick.datetime.hour == 21 and tick.datetime.minute == 0:
                    self.open_flag = False
                # 其他时间段,允许修改开盘价
                else:
                    self.open_flag = True
                    # 如果有上一个tick
                    if self.last_tick:
                        # 如果在创建的时候,也是有成交量的,则不允许修改
                        if tick.volume > self.last_tick.volume:
                            self.open_flag = False
                        # 如果在创建Bar的时候,没有成交量,那么允许修改开盘价
                        else:
                            self.open_flag = True

            self.bar = BarData(
                symbol=tick.symbol,
                exchange=tick.exchange,
                datetime=tick.datetime.replace(second=0, microsecond=0),
                datetime_start=tick.datetime.replace(second=0, microsecond=0),
                datetime_end=tick.datetime.replace(second=0, microsecond=0) + timedelta(minutes=1),

                interval=Interval.MINUTE,
                gateway_name=tick.gateway_name,
                open_price=tick.last_price,
                high_price=tick.last_price,
                low_price=tick.last_price,
                close_price=tick.last_price,
                open_interest=tick.open_interest
            )

        else:
            self.bar.high_price = max(self.bar.high_price, tick.last_price)
            self.bar.low_price = min(self.bar.low_price, tick.last_price)
            self.bar.close_price = tick.last_price
            self.bar.open_interest = tick.open_interest
            # self.bar.datetime = tick.datetime

        if self.last_tick:
            volume_change = tick.volume - self.last_tick.volume
            self.bar.volume += max(volume_change, 0)
            # 如果当前允许修改开盘价
            if self.open_flag:
                # 只有成交价格变动的tick 使得价格改变
                if volume_change:
                    self.bar.open_price = tick.last_price
                    self.bar.high_price = tick.last_price
                    self.bar.low_price = tick.last_price
                    self.open_flag = False

        self.last_tick = tick

    def update_bar(self, bar: BarData):
        """
        Update 1 minute bar into generator
        window_bar更新, 5分钟 10分钟等
        功能就是:输入进来1分钟的Bar,合成5分钟或者15分钟之类的Bar
        改: 将update_bar变成完全的update_bar 分钟Bar 而将Update_bar里面的更新小时Bar提取出来,增加出来
        因为这里的逻辑,本身就是要输入的是小时线,才可以合成更多周期的小时线
        而且这里没有写K线的周期,很不方便
        1 正常情况下
        """
        # If not inited, create window bar object
        # 如果没有window_bar
        dt = bar.datetime.replace(second=0, microsecond=0)
        if self.window == 3:
            period = Interval.MINUTE3
            dt_start = bar.datetime.replace(minute=math.floor(dt.minute / 3) * 3)
            dt_end = dt_start + timedelta(minutes=3)
        elif self.window == 5:
            period = Interval.MINUTE5
            dt_start = bar.datetime.replace(minute=math.floor(dt.minute / 5) * 5)
            dt_end = dt_start + timedelta(minutes=5)
        elif self.window == 15:
            period = Interval.MINUTE15
            dt_start = bar.datetime.replace(minute=math.floor(dt.minute / 15) * 15)
            dt_end = dt_start + timedelta(minutes=15)
        elif self.window == 30:
            period = Interval.MINUTE30
            dt_start = bar.datetime.replace(minute=math.floor(dt.minute / 30) * 30)
            dt_end = dt_start + timedelta(minutes=30)
            # 如果是商品期货,小时==10 分钟 == 0,30分钟线的终值为15
            if (bar.symbol in COMMODITY) and \
                (bar.datetime.hour == 10) and \
                (dt_start.minute == 0):
                dt_end = dt_end.replace(minute=15)
        elif self.window == 60:
            period = Interval.HOUR
            dt_start = bar.datetime.replace(minute=0)
            dt_end = dt_start + timedelta(hours=1)
            if dt_start.hour == 11:
                dt_end = dt_end.replace(minute=30)

            if (dt_start.hour == 13) and (bar.symbol in COMMODITY):
                dt_start = dt_start.replace(minute=30)

            if (dt_start.hour == 23) and (bar.symbol in NIGHTGROUP_2330):
                dt_end = dt_end.replace(minute=30)

            if (dt_start.hour == 2) and (bar.symbol in NIGHTGROUP_0230):
                dt_end = dt_end.replace(minute=30)

        if not self.window_bar:
            # Generate timestamp for bar data
            # 如果是分钟Bar,创建datetime
            # 创建Bar数据
            self.window_bar = BarData(
                symbol=bar.symbol,
                exchange=bar.exchange,
                datetime=dt,
                datetime_start=dt_start,
                datetime_end=dt_end,
                interval=period,
                gateway_name=bar.gateway_name,
                open_price=bar.open_price,
                high_price=bar.high_price,
                low_price=bar.low_price,
                open_interest=bar.open_interest
            )
        # Otherwise, update high/low price into window bar
        # 如果已经进行了初始化,
        else:
            # 这里加入新的合成逻辑，如果是不连续的bar,先推送
            # 如果有跳线的情况
            if bar.datetime >= self.window_bar.datetime_end:
                self.on_window_bar(self.window_bar)
                self.window_bar = None

                self.window_bar = BarData(
                    symbol=bar.symbol,
                    exchange=bar.exchange,
                    datetime=dt,
                    datetime_start=dt_start,
                    datetime_end=dt_end,
                    interval=period,
                    gateway_name=bar.gateway_name,
                    open_price=bar.open_price,
                    high_price=bar.high_price,
                    low_price=bar.low_price,
                    open_interest=bar.open_interest
                )

            self.window_bar.high_price = max(
                self.window_bar.high_price, bar.high_price)
            self.window_bar.low_price = min(
                self.window_bar.low_price, bar.low_price)

        # Update close price/volume into window bar
        # 更新最新价,交易量
        self.window_bar.close_price = bar.close_price
        self.window_bar.volume += int(bar.volume)
        # self.window_bar.open_interest = bar.open_interest

        # Check if window bar completed
        # 先假定没有完成
        finished = False

        # 如果是分钟Bar
        if self.interval == Interval.MINUTE:
            # x-minute bar
            # 如果是5分钟,则规则为: 4 + 1 整除5就推送,如果是15分钟,规则: 14 + 1 整除 15 推送
            # 如果是60分钟,则59+1 % 60 = 0, 其余不为0
            if not ((bar.datetime.minute + 1) % self.window) or \
                    ((bar.datetime + timedelta(minutes=1)) >= self.window_bar.datetime_end):
                finished = True

        if finished:
            self.on_window_bar(self.window_bar)
            self.window_bar = None

        # Cache last bar object
        self.last_bar = bar

    def update_bar_hour(self, bar: BarData):
        """
        这里输入的是小时Bar
        """
        # If not inited, create window bar object
        # 如果没有初始化
        if not self.window_bar:
            # Generate timestamp for bar data
            # 将数据变成整点数据
            dt = bar.datetime.replace(minute=0, second=0, microsecond=0)

            # 创建Bar数据
            if self.window == 2:
                period = Interval.HOUR2
            elif self.window == 4:
                period = Interval.HOUR4
            elif self.window == 6:
                period = Interval.HOUR6

            self.window_bar = BarData(
                symbol=bar.symbol,
                exchange=bar.exchange,
                datetime=dt,
                interval=period,
                gateway_name=bar.gateway_name,
                open_price=bar.open_price,
                high_price=bar.high_price,
                low_price=bar.low_price,
                open_interest=bar.open_interest
            )
        # Otherwise, update high/low price into window bar
        # 如果有初始化,则进行高地价的更新
        else:
            self.window_bar.high_price = max(
                self.window_bar.high_price, bar.high_price)
            self.window_bar.low_price = min(
                self.window_bar.low_price, bar.low_price)

        # Update close price/volume into window bar
        # 更新最新价,交易量
        self.window_bar.close_price = bar.close_price
        self.window_bar.volume += int(bar.volume)
        # self.window_bar.open_interest = bar.open_interest

        # Check if window bar completed
        # 先假定没有完成
        finished = False

        # 如果存在上一个Bar
        if self.last_bar and bar.datetime.hour != self.last_bar.datetime.hour:
            # 1-hour bar
            # 如果是一小时的Bar,则小时Bar推送结束
            # 如果当前的
            if not (bar.datetime.hour + 1) % self.window:
                finished = True
                self.interval_count = 0

        if finished:
            self.on_window_bar(self.window_bar)
            self.window_bar = None

        # Cache last bar object
        self.last_bar = bar

    def generate(self):
        """
        Generate the bar data and call callback immediately.
        强制合成由tick合成的一分钟线,在尾盘的时候用到
        """
        self.bar.datetime = self.bar.datetime.replace(
            second=0, microsecond=0
        )
        self.on_bar(self.bar)
        self.bar = None

    def generate_window_bar(self):
        """
        Generate the window bar data and call callback immediately.
        强制合成由bar合成的分钟线,在尾盘的时候用到
        """
        self.on_window_bar(self.window_bar)
        self.window_bar = None


class ArrayManager(object):
    """
    For:
    1. time series container of bar data
    2. calculating technical indicator value
    """

    def __init__(self, size=100):
        """Constructor"""
        self.count = 0
        self.size = size
        self.inited = False

        self.time_array = []
        self.open_array = np.zeros(size)
        self.high_array = np.zeros(size)
        self.low_array = np.zeros(size)
        self.close_array = np.zeros(size)
        self.volume_array = np.zeros(size)

    def update_bar(self, bar):
        """
        Update new bar data into array manager.
        """
        self.count += 1
        if not self.inited and self.count >= self.size:
            self.inited = True

        if len(self.time_array) >= self.size:
            self.time_array.pop(0)
        self.open_array[:-1] = self.open_array[1:]
        self.high_array[:-1] = self.high_array[1:]
        self.low_array[:-1] = self.low_array[1:]
        self.close_array[:-1] = self.close_array[1:]
        self.volume_array[:-1] = self.volume_array[1:]

        self.time_array.append(bar.datetime)
        self.open_array[-1] = bar.open_price
        self.high_array[-1] = bar.high_price
        self.low_array[-1] = bar.low_price
        self.close_array[-1] = bar.close_price
        self.volume_array[-1] = bar.volume

    @property
    def open(self):
        """
        Get open price time series.
        """
        return self.open_array

    @property
    def high(self):
        """
        Get high price time series.
        """
        return self.high_array

    @property
    def low(self):
        """
        Get low price time series.
        """
        return self.low_array

    @property
    def close(self):
        """
        Get close price time series.
        """
        return self.close_array

    @property
    def volume(self):
        """
        Get trading volume time series.
        """
        return self.volume_array

    def sma(self, n, array=False):
        """
        Simple moving average.
        """
        result = talib.SMA(self.close, n)
        if array:
            return result
        return result[-1]

    def ema(self, n, array=False):
        """
        E moving average.
        """
        result = talib.EMA(self.close, n)
        if array:
            return result
        return result[-1]

    def std(self, n, array=False):
        """
        Standard deviation
        """
        result = talib.STDDEV(self.close, n)
        if array:
            return result
        return result[-1]

    def cci(self, n, array=False):
        """
        Commodity Channel Index (CCI).
        """
        result = talib.CCI(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    def atr(self, n, array=False):
        """
        Average True Range (ATR).
        """
        result = talib.ATR(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    def rsi(self, n, array=False):
        """
        Relative Strenght Index (RSI).
        """
        result = talib.RSI(self.close, n)
        if array:
            return result
        return result[-1]

    def macd(self, fast_period, slow_period, signal_period, array=False):
        """
        MACD.
        """
        macd, signal, hist = talib.MACD(
            self.close, fast_period, slow_period, signal_period
        )
        if array:
            return macd, signal, hist
        return macd[-1], signal[-1], hist[-1]

    def adx(self, n, array=False):
        """
        ADX.
        """
        result = talib.ADX(self.high, self.low, self.close, n)
        if array:
            return result
        return result[-1]

    def boll(self, n, dev, array=False):
        """
        Bollinger Channel.
        """
        mid = self.sma(n, array)
        std = self.std(n, array)

        up = mid + std * dev
        down = mid - std * dev

        return up, down

    def keltner(self, n, dev, array=False):
        """
        Keltner Channel.
        """
        mid = self.sma(n, array)
        atr = self.atr(n, array)

        up = mid + atr * dev
        down = mid - atr * dev

        return up, down

    def donchian(self, n, array=False):
        """
        Donchian Channel.
        """
        up = talib.MAX(self.high, n)
        down = talib.MIN(self.low, n)

        if array:
            return up, down
        return up[-1], down[-1]


def virtual(func: "callable"):
    """
    mark a function as "virtual", which means that this function can be override.
    any base class should use this or @abstractmethod to decorate all functions
    that can be (re)implemented by subclasses.
    """
    return func

# 输入tick数据或分钟数据的时间（datetime对象），品种，以及周期，得到交易日的  起始时间，终止时间，交易日期
def timeStartEnd(time: datetime, instrument: str, interval: Interval):
    # 夜盘品种，先解决tradeday
    if time.hour >= 21 and time.hour <= 23:
        # 周五晚上
        if time.weekday() == 4:
            tradeday = (time + timedelta(days=2, hours=5)).replace(hour=0)
        else:
            tradeday = (time + timedelta(hours=5)).replace(hour=0)
    elif time.hour < 3:
        # 周六凌晨
        if time.weekday() == 5:
            tradeday = (time + timedelta(days=2, hours=5)).replace(hour=0)
        else:
            tradeday = (time + timedelta(hours=5)).replace(hour=0)
    elif time.hour >= 9 and time.hour <= 15:
        tradeday = time.replace(hour=0)
    else:
        print('输入的时间不在交易范围之内，请检查')
        tradeday=None

    tradeday = tradeday.replace(hour=9)

    # 解决start 和 end的问题
    # 1分钟K线采用闭开区间定义start 和 end
    tradeday = tradeday.replace(second=0, microsecond=0)
    if interval == Interval.MINUTE:
        timestart = time.replace(second=0, microsecond=0)
        timeend = timestart + timedelta(minutes=1)
        return timestart, timeend, tradeday
    elif interval == Interval.MINUTE5:
        minstart = math.floor(time.minute / 5) * 5
        timestart = time.replace(minute=minstart)
        timeend = timestart + timedelta(minutes=5)
        return timestart, timeend, tradeday
    elif interval == Interval.MINUTE15:
        minstart = math.floor(time.minute / 15) * 15
        timestart = time.replace(minute=minstart)
        timeend = timestart + timedelta(minutes=15)
        return timestart, timeend, tradeday
    elif interval == INTERVAL_30M:
        minstart = math.floor(time.minute / 30) * 30
        timestart = time.replace(minute=minstart)
        timeend = timestart + timedelta(minutes=29, seconds=59)
        # 30分钟的K线起始终止时间节点，要代替一下
        if timeend.hour == 10 and timeend.minute == 29:
            timeend = timeend.replace(minute=14)
        return timestart, timeend, tradeday
    # 60分钟这里
    elif interval == INTERVAL_60M:
        timestart = time.replace(minute=0)
        timeend = timestart + timedelta(minutes=59, seconds=59)
        # 30分钟的K线起始终止时间节点，要代替一下
        if timeend.hour == 11 and timeend.minute == 59:
            timeend = timeend.replace(minute=29)
        elif timeend.hour == 2 and timeend.minute == 59:
            timeend = timeend.replace(minute=29)
        elif timeend.hour == 23 and timeend.minute == 59:
            # 晚上23点的这个，很麻烦，之前大商所是凌晨1点闭市，之后改为晚上23点半闭市
            if instrument == 'j':
                if time.strftime('%Y-%m-%d') >= '2015-05-08':
                    timeend = timeend.replace(minute=29)
        return timestart, timeend, tradeday
    elif interval == INTERVAL_1D:
        # 夜盘品种
        timeend = tradeday.replace(hour=14, minute=59, second=59)
        # 夜盘品种
        if instrument in ['rb', 'j', 'i', 'jm']:
            if time.hour >= 21 and time.hour <= 23:
                timestart = time.replace(hour=21)
            elif time.hour < 3:
                timestart = (time - timedelta(hours=5)).replace(hour=21)
            # 如果是夜盘品种，但是前一天没有数据，那么默认假日模式，
            elif time.hour >= 9 and time.hour <= 15:
                if time.weekday() == 0:
                    timestart = (time - timedelta(days=3)).replace(hour=21)
                else:
                    timestart = (time - timedelta(days=1)).replace(hour=21)
            else:
                print('输入的时间不在交易范围之内，请检查')
        # 日盘品种
        else:
            timestart = time.replace(hour=9)
    else:
        print('输入的时间周期有误，请重新输入')
        timestart = None
        timeend = None

    return timestart, timeend, tradeday