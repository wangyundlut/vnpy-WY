from vnpy.app.cta_strategy import (
    CtaTemplate,
    StopOrder,
    TickData,
    BarData,
    TradeData,
    OrderData,
    BarGenerator,
    ArrayManager,
)
# 这个策略用来收集K线和测试K线用的


class DoubleMaStrategy(CtaTemplate):
    author = "用Python的交易员"

    fast_window = 10
    slow_window = 20

    fast_ma0 = 0.0
    fast_ma1 = 0.0

    slow_ma0 = 0.0
    slow_ma1 = 0.0

    parameters = ["fast_window", "slow_window"]
    variables = ["fast_ma0", "fast_ma1", "slow_ma0", "slow_ma1"]

    def __init__(self, cta_engine, strategy_name, vt_symbol, setting):
        """"""
        super(DoubleMaStrategy, self).__init__(
            cta_engine, strategy_name, vt_symbol, setting
        )

        self.am1 = ArrayManager()

        self.bg5 = BarGenerator(self.on_bar, 5, self.on_5min_bar)
        self.am5 = ArrayManager()

        self.bg15 = BarGenerator(self.on_bar, 15, self.on_15min_bar)
        self.am15 = ArrayManager()

        self.bg30 = BarGenerator(self.on_bar, 30, self.on_30min_bar)
        self.am30 = ArrayManager()

        self.bg60 = BarGenerator(self.on_bar, 60, self.on_60min_bar)
        self.am60 = ArrayManager()

    def on_init(self):
        """
        Callback when strategy is inited.
        """
        self.write_log("策略初始化")
        self.load_bar(10)

    def on_start(self):
        """
        Callback when strategy is started.
        """
        self.write_log("策略启动")
        self.put_event()

    def on_stop(self):
        """
        Callback when strategy is stopped.
        """
        self.write_log("策略停止")

        self.put_event()

    def on_tick(self, tick: TickData):
        """
        Callback of new tick data update.
        """
        pass
        # self.bg.update_tick(tick)

    def on_bar(self, bar: BarData):
        """
        Callback of new bar data update.
        """
        # print(bar.__dict__)
        am1 = self.am1
        if am1.count != 0:
            # 只有时间符合的才更新
            if bar.datetime > am1.time_array[-1]:
                am1.update_bar(bar)
        else:
            am1.update_bar(bar)
        # self.write_log("double_ma_strategy 1分钟Bar" + str(bar.__dict__))
        # 产生5分钟的bar
        self.bg5.update_bar(bar)
        # 产生15分钟的bar
        self.bg15.update_bar(bar)
        # 产生30分钟的bar
        self.bg30.update_bar(bar)
        # 产生60分钟的bar
        self.bg60.update_bar(bar)

        am = am1
        fast_ma = am.ema(self.fast_window, array=True)
        self.fast_ma0 = fast_ma[-1]
        self.fast_ma1 = fast_ma[-2]

        slow_ma = am.ema(self.slow_window, array=True)
        self.slow_ma0 = slow_ma[-1]
        self.slow_ma1 = slow_ma[-2]

        cross_over = self.fast_ma0 > self.slow_ma0 and self.fast_ma1 < self.slow_ma1
        cross_below = self.fast_ma0 < self.slow_ma0 and self.fast_ma1 > self.slow_ma1

    def on_5min_bar(self, bar: BarData):
        am5 = self.am5
        if am5.count != 0:
            # 只有时间符合的才更新
            if bar.datetime > am5.time_array[-1]:
                am5.update_bar(bar)
                # self.write_log("double_ma_strategy 5分钟Bar" + str(bar.__dict__))
            if not am5.inited:
                return
        else:
            am5.update_bar(bar)

    def on_15min_bar(self, bar: BarData):

        am15 = self.am15
        if am15.count != 0:
            # 只有时间符合的才更新
            if bar.datetime > am15.time_array[-1]:
                am15.update_bar(bar)
                # self.write_log("double_ma_strategy 15分钟Bar" + str(bar.__dict__))
            if not am15.inited:
                return
        else:
            am15.update_bar(bar)

    def on_30min_bar(self, bar: BarData):
        # 策略是在30分钟的周期上进行逻辑判断的
        # self.write_log("double_ma_strategy 30分钟Bar" + str(bar.__dict__))
        # 先检查数据收录情况
        am30 = self.am30
        if am30.count != 0:
            # 只有时间符合的才更新
            if bar.datetime > am30.time_array[-1]:
                am30.update_bar(bar)
                # self.write_log("double_ma_strategy 30分钟Bar" + str(bar.__dict__))
        else:
            am30.update_bar(bar)
        if not am30.inited:
            return

    def on_60min_bar(self, bar: BarData):
        # self.write_log("double_ma_strategy 1小时Bar" + str(bar.__dict__))
        # 先检查数据收录情况
        am60 = self.am60
        if am60.count != 0:
            # 只有时间符合的才更新
            if bar.datetime > am60.time_array[-1]:
                am60.update_bar(bar)
        else:
            am60.update_bar(bar)
        if not am60.inited:
            return

        am = am60
        fast_ma = am.sma(self.fast_window, array=True)
        self.fast_ma0 = fast_ma[-1]
        self.fast_ma1 = fast_ma[-2]

        slow_ma = am.sma(self.slow_window, array=True)
        self.slow_ma0 = slow_ma[-1]
        self.slow_ma1 = slow_ma[-2]

        cross_over = self.fast_ma0 > self.slow_ma0 and self.fast_ma1 < self.slow_ma1
        cross_below = self.fast_ma0 < self.slow_ma0 and self.fast_ma1 > self.slow_ma1
        """
        if cross_over:
            if self.pos == 0:
                self.buy(bar.close_price, 1)
            elif self.pos < 0:
                self.cover(bar.close_price, 1)
                self.buy(bar.close_price, 1)

        elif cross_below:
            if self.pos == 0:
                self.short(bar.close_price, 1)
            elif self.pos > 0:
                self.sell(bar.close_price, 1)
                self.short(bar.close_price, 1)
        """
        self.put_event()

    def on_order(self, order: OrderData):
        """
        Callback of new order data update.
        """
        pass

    def on_trade(self, trade: TradeData):
        """
        Callback of new trade data update.
        """
        self.put_event()

    def on_stop_order(self, stop_order: StopOrder):
        """
        Callback of stop order update.
        """
        pass
