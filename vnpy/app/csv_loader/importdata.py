import time
import pymongo
import csv
from vnpy.trader.object import BarData
from vnpy.trader.constant import Exchange, Interval
from datetime import datetime, timedelta


def example1():
    file_name = "C:\\TB数据\\m2001.csv"
    db_name = 'BarData'
    symbol = "m2001"
    load_csv(file_name, db_name, symbol)


def load_csv(fileName, dbName, symbol):
    """将TradeBlazer导出的csv格式的历史分钟数据插入到Mongo数据库中"""
    start = time.time()
    print(u'开始读取CSV文件%s中的数据插入到%s的%s中' % (fileName, dbName, symbol))

    # 创建数据库
    client = pymongo.MongoClient("localhost", 27017)
    collection = client[dbName][symbol]
    collection.create_index([('datetime', pymongo.ASCENDING)], unique=True)

    # 读取数据和插入到数据库
    # reader = csv.reader(file(fileName, 'r'))
    reader = csv.reader(open(fileName, 'r'))

    for d in reader:
        bar_datetime = datetime.strptime(d[0], "%Y/%m/%d %H:%M")
        bar = BarData(gateway_name="CTP",
                      symbol=symbol,
                      exchange=Exchange.DCE,
                      datetime=bar_datetime,
                      datetime_start=bar_datetime,
                      datetime_end=bar_datetime + timedelta(minutes=1))

        # bar.trade_day = bar_datetime.strftime("%Y-%m-%d")
        bar.interval = Interval.MINUTE

        bar.open_price = float(d[1])
        bar.high_price = float(d[2])
        bar.low_price = float(d[3])
        bar.close_price = float(d[4])

        bar.volume = float(d[5])
        bar.open_interest = float(d[6])

        d = bar.__dict__
        d["exchange"] = d["exchange"].value
        d["interval"] = d["interval"].value

        flt = {'datetime': bar.datetime}
        collection.update_one(flt, {'$set': bar.__dict__}, upsert=True)
        print(bar.datetime)

    print(u'插入完毕，耗时：%s' % (time.time() - start))


if __name__ == "__main__":
    example1()