"""
"""

from datetime import datetime, timedelta
import re

from vnpy.api.ctp import (
    MdApi,
    TdApi,
    THOST_FTDC_OAS_Submitted,
    THOST_FTDC_OAS_Accepted,
    THOST_FTDC_OAS_Rejected,
    THOST_FTDC_OST_NoTradeQueueing,
    THOST_FTDC_OST_PartTradedQueueing,
    THOST_FTDC_OST_AllTraded,
    THOST_FTDC_OST_Canceled,
    THOST_FTDC_D_Buy, 
    THOST_FTDC_D_Sell,
    THOST_FTDC_PD_Long,
    THOST_FTDC_PD_Short,
    THOST_FTDC_OPT_LimitPrice,
    THOST_FTDC_OPT_AnyPrice,
    THOST_FTDC_OF_Open,
    THOST_FTDC_OFEN_Close,
    THOST_FTDC_OFEN_CloseYesterday,
    THOST_FTDC_OFEN_CloseToday,
    THOST_FTDC_PC_Futures,
    THOST_FTDC_PC_Options,
    THOST_FTDC_PC_Combination,
    THOST_FTDC_CP_CallOptions,
    THOST_FTDC_CP_PutOptions,
    THOST_FTDC_HF_Speculation,
    THOST_FTDC_CC_Immediately,
    THOST_FTDC_FCC_NotForceClose,
    THOST_FTDC_TC_GFD,
    THOST_FTDC_VC_AV,
    THOST_FTDC_TC_IOC,
    THOST_FTDC_VC_CV,
    THOST_FTDC_AF_Delete
)
from vnpy.trader.constant import (
    Direction,
    Offset,
    Exchange,
    OrderType,
    Product,
    Status,
    OptionType
)
from vnpy.trader.gateway import BaseGateway
from vnpy.trader.object import (
    TickData,
    OrderData,
    TradeData,
    PositionData,
    AccountData,
    ContractData,
    OrderRequest,
    CancelRequest,
    SubscribeRequest,
    BarData,
)
from vnpy.trader.utility import get_folder_path
from vnpy.trader.event import EVENT_TIMER, EVENT_BAR
from vnpy.trader.utility import BarGenerator


STATUS_CTP2VT = {
    THOST_FTDC_OAS_Submitted: Status.SUBMITTING,
    THOST_FTDC_OAS_Accepted: Status.SUBMITTING,
    THOST_FTDC_OAS_Rejected: Status.REJECTED,
    THOST_FTDC_OST_NoTradeQueueing: Status.NOTTRADED,
    THOST_FTDC_OST_PartTradedQueueing: Status.PARTTRADED,
    THOST_FTDC_OST_AllTraded: Status.ALLTRADED,
    THOST_FTDC_OST_Canceled: Status.CANCELLED
}

DIRECTION_VT2CTP = {
    Direction.LONG: THOST_FTDC_D_Buy, 
    Direction.SHORT: THOST_FTDC_D_Sell
}
DIRECTION_CTP2VT = {v: k for k, v in DIRECTION_VT2CTP.items()}
DIRECTION_CTP2VT[THOST_FTDC_PD_Long] = Direction.LONG
DIRECTION_CTP2VT[THOST_FTDC_PD_Short] = Direction.SHORT

ORDERTYPE_VT2CTP = {
    OrderType.LIMIT: THOST_FTDC_OPT_LimitPrice, 
    OrderType.MARKET: THOST_FTDC_OPT_AnyPrice
}
ORDERTYPE_CTP2VT = {v: k for k, v in ORDERTYPE_VT2CTP.items()}

OFFSET_VT2CTP = {
    Offset.OPEN: THOST_FTDC_OF_Open, 
    Offset.CLOSE: THOST_FTDC_OFEN_Close,
    Offset.CLOSETODAY: THOST_FTDC_OFEN_CloseToday,
    Offset.CLOSEYESTERDAY: THOST_FTDC_OFEN_CloseYesterday,
}
OFFSET_CTP2VT = {v: k for k, v in OFFSET_VT2CTP.items()}

EXCHANGE_CTP2VT = {
    "CFFEX": Exchange.CFFEX,
    "SHFE": Exchange.SHFE,
    "CZCE": Exchange.CZCE,
    "DCE": Exchange.DCE,
    "INE": Exchange.INE
}

PRODUCT_CTP2VT = {
    THOST_FTDC_PC_Futures: Product.FUTURES,
    THOST_FTDC_PC_Options: Product.OPTION,
    THOST_FTDC_PC_Combination: Product.SPREAD
}

OPTIONTYPE_CTP2VT = {
    THOST_FTDC_CP_CallOptions: OptionType.CALL,
    THOST_FTDC_CP_PutOptions: OptionType.PUT
}


symbol_exchange_map = {}
symbol_name_map = {}
symbol_size_map = {}
# 增
symbol_price_map = {}
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


class CtpGateway(BaseGateway):
    """
    VN Trader Gateway for CTP .
    """

    default_setting = {
        "用户名": "",
        "密码": "",
        "经纪商代码": "",
        "交易服务器": "",
        "行情服务器": "",
        "产品名称": "",
        "授权编码": "",
        "产品信息": ""
    }

    exchanges = list(EXCHANGE_CTP2VT.values())
    
    def __init__(self, event_engine):
        """Constructor"""
        super().__init__(event_engine, "CTP")

        self.td_api = CtpTdApi(self)
        self.md_api = CtpMdApi(self)
        self.timer_count = 0
        self.start_tick = False
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)

    def connect(self, setting: dict):
        """"""
        userid = setting["用户名"]
        password = setting["密码"]
        brokerid = setting["经纪商代码"]
        td_address = setting["交易服务器"]
        md_address = setting["行情服务器"]
        appid = setting["产品名称"]
        auth_code = setting["授权编码"]
        product_info = setting["产品信息"]
        
        if not td_address.startswith("tcp://"):
            td_address = "tcp://" + td_address
        if not md_address.startswith("tcp://"):
            md_address = "tcp://" + md_address
        
        self.td_api.connect(td_address, userid, password, brokerid, auth_code, appid, product_info)
        self.md_api.connect(md_address, userid, password, brokerid)
        
        # self.init_query()

    def subscribe(self, req: SubscribeRequest):
        """"""
        self.md_api.subscribe(req)

    def send_order(self, req: OrderRequest):
        """"""
        return self.td_api.send_order(req)

    def cancel_order(self, req: CancelRequest):
        """"""
        self.td_api.cancel_order(req)

    def query_account(self):
        """"""
        self.td_api.query_account()

    def query_position(self):
        """"""
        self.td_api.query_position()

    def close(self):
        """"""
        self.td_api.close()
        self.md_api.close()

    def write_error(self, msg: str, error: dict):
        """"""
        error_id = error["ErrorID"]
        error_msg = error["ErrorMsg"]
        msg = f"{msg}，代码：{error_id}，信息：{error_msg}"
        self.write_log(msg)        

    def process_timer_event(self, event):
        """"""
        # 20秒检查一次
        self.timer_count += 1
        if self.timer_count < 20:
            return
        self.timer_count = 0

        # 获取当前时间戳
        current_time = datetime.now()
        if not self.start_tick:
            # 只有正常开启的行情,才会接受数据,非正常服务器时间接受的数据,不作数
            if current_time.hour == 8:
                if current_time.minute >= 50:
                    self.md_api.trading = True
                    self.start_tick = True
            if current_time.hour == 20:
                if current_time.minute >= 50:
                    self.md_api.trading = True
                    self.start_tick = True

        # 当前时间与tick最后一个时间的对比,如果有30秒未更新,则强制合成K线
        for symbol, tick_time in self.md_api.tick_last_time_dict.items():
            # 首次,没有时间
            if not tick_time:
                continue
            # 如果是已经更新完了的bar
            elif not self.md_api.bg_dict[symbol].bar:
                continue
            # 如果接下来的时间差额大于20秒,强制生成一分钟Bar
            elif current_time - tick_time > timedelta(seconds=60):
                self.md_api.bg_dict[symbol].generate()

        """
        self.count += 1
        if self.count < 2:
            return
        self.count = 0
        
        func = self.query_functions.pop(0)
        func()
        self.query_functions.append(func)
        """
    """
    def init_query(self):
        """"""
        self.count = 0
        self.query_functions = [self.query_account, self.query_position]
        self.event_engine.register(EVENT_TIMER, self.process_timer_event)
    """


class CtpMdApi(MdApi):
    """"""

    def __init__(self, gateway):
        """Constructor"""
        super(CtpMdApi, self).__init__()
        
        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        
        self.reqid = 0
        
        self.connect_status = False
        self.login_status = False
        self.subscribed = set()
        
        self.userid = ""
        self.password = ""
        self.brokerid = ""

        # 增
        self.date = datetime.now().strftime("%Y%m%d")
        # K线合成容器,gateway推出一分钟K线
        self.bg_dict = {}
        # tick最后时间,用于生成一分钟K线
        self.tick_last_time_dict = {}

        self.trading = False
    
    def onFrontConnected(self):
        """
        Callback when front server is connected.
        """
        self.gateway.write_log("行情服务器连接成功")
        self.login()

    def onFrontDisconnected(self, reason: int):
        """
        Callback when front server is disconnected.
        """
        self.login_status = False
        self.gateway.write_log(f"行情服务器连接断开，原因{reason}")

    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool):
        """
        Callback when user is logged in.
        """
        if not error["ErrorID"]:
            self.login_status = True
            self.gateway.write_log("行情服务器登录成功")
            
            for symbol in self.subscribed:
                self.subscribeMarketData(symbol)
        else:
            self.gateway.write_error("行情服务器登录失败", error)
    
    def onRspError(self, error: dict, reqid: int, last: bool):
        """
        Callback when error occured.
        """
        self.gateway.write_error("行情接口报错", error)
    
    def onRspSubMarketData(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        if not error or not error["ErrorID"]:
            return
        
        self.gateway.write_error("行情订阅失败", error)

    def onRtnDepthMarketData(self, data: dict):
        """
        Callback of tick data update.
        """
        symbol = data["InstrumentID"]
        exchange = symbol_exchange_map.get(symbol, "")
        # 增加
        price_tick = symbol_price_map.get(symbol, 0.0001)
        if not exchange:
            return

        if exchange == Exchange.DCE:
            timestamp = f"{self.date} {data['UpdateTime']}.{int(data['UpdateMillisec']/100)}"
        else:
            timestamp = f"{data['ActionDay']} {data['UpdateTime']}.{int(data['UpdateMillisec']/100)}"
        
        tick = TickData(
            symbol=symbol,
            exchange=exchange,
            datetime=datetime.strptime(timestamp, "%Y%m%d %H:%M:%S.%f"),
            name=symbol_name_map[symbol],
            volume=data["Volume"],
            open_interest=data["OpenInterest"],
            last_price=round_to(data["LastPrice"], price_tick),
            limit_up=round_to(data["UpperLimitPrice"], price_tick),
            limit_down=round_to(data["LowerLimitPrice"], price_tick),
            open_price=round_to(data["OpenPrice"], price_tick),
            high_price=round_to(data["HighestPrice"], price_tick),
            low_price=round_to(data["LowestPrice"], price_tick),
            pre_close=round_to(data["PreClosePrice"], price_tick),
            bid_price_1=round_to(data["BidPrice1"], price_tick),
            ask_price_1=round_to(data["AskPrice1"], price_tick),
            bid_volume_1=data["BidVolume1"],
            ask_volume_1=data["AskVolume1"],
            gateway_name=self.gateway_name
        )
        # 郑商所会在8点45分的时候推送一个23:29的tick,这个tick,上面的逻辑过滤不掉
        if not self.trading:
            return

        # 获取例如rb代码
        symb = re.sub(r'\d', '', symbol).lower()
        # 时间过滤，由于各个交易所品种不同，收盘时间不同
        # 有些会在截止的时候发出TICK23:30:00（大商所）
        # 有些不会在截止的时候发出TICK（郑商所）
        # 这里将格式统一，统一不收取品种收盘时的TICK，然后自己合成
        # 理论上，不会改变K线，最后一个TICK只是一些交易所用来确认时间点的
        # 先处理通用时间窗口
        tick_time = tick.datetime.strftime("%H:%M:%S")
        if (tick_time >= '02:30:00') & (tick_time < '09:00:00'):
            return
        if (tick_time >= '10:15:00') & (tick_time < '10:30:00') & (symb in COMMODITY):
            return
        if (tick_time >= '11:30:00') & (tick_time < '13:30:00'):
            return
        if (tick_time >= '15:00:00') & (tick_time < '21:00:00'):
            return
        if (symb in NIGHTGROUP_2300) and tick_time >= '23:00:00':
            return
        # 时间和品种一起过滤
        if (symb in NIGHTGROUP_2330) and tick_time >= '23:30:00':
            return
        if symb in NIGHTGROUP_0100:
            current_time = datetime.now().strftime('%H:%M:%S')
            # 当前是夜盘第二个交易日
            if current_time <= '02:30:00':
                # 当前时间超过最后一个TICK
                if tick_time >= '01:00:00':
                    return
        # 推送出去tick
        self.gateway.on_tick(tick)
        bg = self.bg_dict.get(tick.symbol, None)
        if bg:
            bg.update_tick(tick)
        else:
            self.bg_dict[tick.symbol] = BarGenerator(self.on_bar)
            self.bg_dict[tick.symbol].update_tick(tick)

        self.tick_last_time_dict[tick.symbol] = tick.datetime

    def on_bar(self, bar: BarData):
        self.gateway.on_bar(bar)

    def connect(self, address: str, userid: str, password: str, brokerid: int):
        """
        Start connection to server.
        """
        self.userid = userid
        self.password = password
        self.brokerid = brokerid
        
        # If not connected, then start connection first.
        if not self.connect_status:
            path = get_folder_path(self.gateway_name.lower())
            self.createFtdcMdApi(str(path) + "\\Md")
            
            self.registerFront(address)
            self.init()

            self.connect_status = True
        # If already connected, then login immediately.
        elif not self.login_status:
            self.login()
    
    def login(self):
        """
        Login onto server.
        """
        req = {
            "UserID": self.userid,
            "Password": self.password,
            "BrokerID": self.brokerid
        }
        
        self.reqid += 1
        self.reqUserLogin(req, self.reqid)
        
    def subscribe(self, req: SubscribeRequest):
        """
        Subscribe to tick data update.
        """
        if self.login_status:
            self.subscribeMarketData(req.symbol)
        self.subscribed.add(req.symbol)
    
    def close(self):
        """
        Close the connection.
        """
        if self.connect_status:
            self.exit()


class CtpTdApi(TdApi):
    """"""

    def __init__(self, gateway):
        """Constructor"""
        super(CtpTdApi, self).__init__()
        
        self.gateway = gateway
        self.gateway_name = gateway.gateway_name
        
        self.reqid = 0
        self.order_ref = 0
        
        self.connect_status = False
        self.login_status = False
        self.auth_staus = False
        self.login_failed = False
        
        self.userid = ""
        self.password = ""
        self.brokerid = ""
        self.auth_code = ""
        self.appid = ""
        self.product_info = ""
        
        self.frontid = 0
        self.sessionid = 0
        
        self.order_data = []
        self.trade_data = []
        self.positions = {}
        self.sysid_orderid_map = {}
        
    def onFrontConnected(self):
        """"""
        self.gateway.write_log("交易服务器连接成功")
        
        if self.auth_code:
            self.authenticate()
        else:
            self.login()
    
    def onFrontDisconnected(self, reason: int):
        """"""
        self.login_status = False
        self.gateway.write_log(f"交易服务器连接断开，原因{reason}")        
    
    def onRspAuthenticate(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        if not error['ErrorID']:
            self.auth_staus = True
            self.gateway.write_log("交易服务器授权验证成功")
            self.login()
        else:
            self.gateway.write_error("交易服务器授权验证失败", error)
    
    def onRspUserLogin(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        if not error["ErrorID"]:
            self.frontid = data["FrontID"]
            self.sessionid = data["SessionID"]
            self.login_status = True
            self.gateway.write_log("交易服务器登录成功")
            
            # Confirm settlement
            req = {
                "BrokerID": self.brokerid,
                "InvestorID": self.userid
            }
            self.reqid += 1
            self.reqSettlementInfoConfirm(req, self.reqid)
        else:
            self.login_failed = True
            
            self.gateway.write_error("交易服务器登录失败", error)
    
    def onRspOrderInsert(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        order_ref = data["OrderRef"]
        orderid = f"{self.frontid}_{self.sessionid}_{order_ref}"
        
        symbol = data["InstrumentID"]
        exchange = symbol_exchange_map[symbol]
        
        order = OrderData(
            symbol=symbol,
            exchange=exchange,
            orderid=orderid,
            direction=DIRECTION_CTP2VT[data["Direction"]],
            offset=OFFSET_CTP2VT[data["CombOffsetFlag"]],
            price=data["LimitPrice"],
            volume=data["VolumeTotalOriginal"],
            status=Status.REJECTED,
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order)
        
        self.gateway.write_error("交易委托失败", error)
    
    def onRspOrderAction(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        self.gateway.write_error("交易撤单失败", error)
    
    def onRspQueryMaxOrderVolume(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        pass
    
    def onRspSettlementInfoConfirm(self, data: dict, error: dict, reqid: int, last: bool):
        """
        Callback of settlment info confimation.
        """
        self.gateway.write_log("结算信息确认成功")
        
        self.reqid += 1
        self.reqQryInstrument({}, self.reqid)
    
    def onRspQryInvestorPosition(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        if not data:
            return
        
        # Get buffered position object
        key = f"{data['InstrumentID'], data['PosiDirection']}"
        position = self.positions.get(key, None)
        if not position:
            position = PositionData(
                symbol=data["InstrumentID"],
                exchange=symbol_exchange_map[data["InstrumentID"]],
                direction=DIRECTION_CTP2VT[data["PosiDirection"]],
                gateway_name=self.gateway_name
            )
            self.positions[key] = position
        
        # For SHFE position data update
        if position.exchange == Exchange.SHFE:
            if data["YdPosition"] and not data["TodayPosition"]:
                position.yd_volume = data["Position"]
        # For other exchange position data update
        else:
            position.yd_volume = data["Position"] - data["TodayPosition"]
        
        # Get contract size (spread contract has no size value)
        size = symbol_size_map.get(position.symbol, 0)
        
        # Calculate previous position cost
        cost = position.price * position.volume * size
        
        # Update new position volume
        position.volume += data["Position"]
        position.pnl += data["PositionProfit"]
        
        # Calculate average position price
        if position.volume and size:
            cost += data["PositionCost"]
            position.price = cost / (position.volume * size)
        
        # Get frozen volume
        if position.direction == Direction.LONG:
            position.frozen += data["ShortFrozen"]
        else:
            position.frozen += data["LongFrozen"]
        
        if last:
            for position in self.positions.values():
                self.gateway.on_position(position)
                
            self.positions.clear()
    
    def onRspQryTradingAccount(self, data: dict, error: dict, reqid: int, last: bool):
        """"""
        account = AccountData(
            accountid=data["AccountID"],
            balance=data["Balance"],
            frozen=data["FrozenMargin"] + data["FrozenCash"] + data["FrozenCommission"],
            gateway_name=self.gateway_name
        )
        account.available = data["Available"]
        
        self.gateway.on_account(account)
    
    def onRspQryInstrument(self, data: dict, error: dict, reqid: int, last: bool):
        """
        Callback of instrument query.
        """
        product = PRODUCT_CTP2VT.get(data["ProductClass"], None)
        if product:            
            contract = ContractData(
                symbol=data["InstrumentID"],
                exchange=EXCHANGE_CTP2VT[data["ExchangeID"]],
                name=data["InstrumentName"],
                product=product,
                size=data["VolumeMultiple"],
                pricetick=data["PriceTick"],
                gateway_name=self.gateway_name
            )
            
            # For option only
            if contract.product == Product.OPTION:
                contract.option_underlying = data["UnderlyingInstrID"],
                contract.option_type = OPTIONTYPE_CTP2VT.get(data["OptionsType"], None),
                contract.option_strike = data["StrikePrice"],
                contract.option_expiry = datetime.strptime(data["ExpireDate"], "%Y%m%d"),
            
            self.gateway.on_contract(contract)
            
            symbol_exchange_map[contract.symbol] = contract.exchange
            symbol_name_map[contract.symbol] = contract.name
            symbol_size_map[contract.symbol] = contract.size
            # 增
            symbol_price_map[contract.symbol] = contract.pricetick
        
        if last:
            self.gateway.write_log("合约信息查询成功")
            
            for data in self.order_data:
                self.onRtnOrder(data)
            self.order_data.clear()
            
            for data in self.trade_data:
                self.onRtnTrade(data)
            self.trade_data.clear()
    
    def onRtnOrder(self, data: dict):
        """
        Callback of order status update.
        """
        symbol = data["InstrumentID"]
        exchange = symbol_exchange_map.get(symbol, "")
        if not exchange:
            self.order_data.append(data)
            return
        
        frontid = data["FrontID"]
        sessionid = data["SessionID"]
        order_ref = data["OrderRef"]
        orderid = f"{frontid}_{sessionid}_{order_ref}"
        
        order = OrderData(
            symbol=symbol,
            exchange=exchange,
            orderid=orderid,
            type=ORDERTYPE_CTP2VT[data["OrderPriceType"]],
            direction=DIRECTION_CTP2VT[data["Direction"]],
            offset=OFFSET_CTP2VT[data["CombOffsetFlag"]],
            price=data["LimitPrice"],
            volume=data["VolumeTotalOriginal"],
            traded=data["VolumeTraded"],
            status=STATUS_CTP2VT[data["OrderStatus"]],
            time=data["InsertTime"],
            gateway_name=self.gateway_name
        )
        self.gateway.on_order(order)
        
        self.sysid_orderid_map[data["OrderSysID"]] = orderid
    
    def onRtnTrade(self, data: dict):
        """
        Callback of trade status update.
        """
        symbol = data["InstrumentID"]
        exchange = symbol_exchange_map.get(symbol, "")
        if not exchange:
            self.trade_data.append(data)
            return

        orderid = self.sysid_orderid_map[data["OrderSysID"]]
        
        trade = TradeData(
            symbol=symbol,
            exchange=exchange,
            orderid=orderid,
            tradeid=data["TradeID"],
            direction=DIRECTION_CTP2VT[data["Direction"]],
            offset=OFFSET_CTP2VT[data["OffsetFlag"]],
            price=data["Price"],
            volume=data["Volume"],
            time=data["TradeTime"],
            gateway_name=self.gateway_name
        )
        self.gateway.on_trade(trade)        
    
    def connect(
        self, 
        address: str, 
        userid: str, 
        password: str, 
        brokerid: int, 
        auth_code: str, 
        appid: str,
        product_info
    ):
        """
        Start connection to server.
        """
        self.userid = userid
        self.password = password
        self.brokerid = brokerid
        self.auth_code = auth_code
        self.appid = appid
        self.product_info = product_info
        
        if not self.connect_status:
            path = get_folder_path(self.gateway_name.lower())
            self.createFtdcTraderApi(str(path) + "\\Td")
            
            self.subscribePrivateTopic(0)
            self.subscribePublicTopic(0)
            
            self.registerFront(address)
            self.init()            

            self.connect_status = True
        else:
            self.authenticate()
    
    def authenticate(self):
        """
        Authenticate with auth_code and appid.
        """
        req = {
            "UserID": self.userid,
            "BrokerID": self.brokerid,
            "AuthCode": self.auth_code,
            "AppID": self.appid
        }

        if self.product_info:
            req["UserProductInfo"] = self.product_info
        
        self.reqid += 1
        self.reqAuthenticate(req, self.reqid)
    
    def login(self):
        """
        Login onto server.
        """
        if self.login_failed:
            return

        req = {
            "UserID": self.userid,
            "Password": self.password,
            "BrokerID": self.brokerid,
            "AppID": self.appid
        }

        if self.product_info:
            req["UserProductInfo"] = self.product_info
        
        self.reqid += 1
        self.reqUserLogin(req, self.reqid)
        
    def send_order(self, req: OrderRequest):
        """
        Send new order.
        """
        self.order_ref += 1
        
        ctp_req = {
            "InstrumentID": req.symbol,
            "LimitPrice": req.price,
            "VolumeTotalOriginal": int(req.volume),
            "OrderPriceType": ORDERTYPE_VT2CTP.get(req.type, ""),
            "Direction": DIRECTION_VT2CTP.get(req.direction, ""),
            "CombOffsetFlag": OFFSET_VT2CTP.get(req.offset, ""),
            "OrderRef": str(self.order_ref),
            "InvestorID": self.userid,
            "UserID": self.userid,
            "BrokerID": self.brokerid,
            "CombHedgeFlag": THOST_FTDC_HF_Speculation,
            "ContingentCondition": THOST_FTDC_CC_Immediately,
            "ForceCloseReason": THOST_FTDC_FCC_NotForceClose,
            "IsAutoSuspend": 0,
            "TimeCondition": THOST_FTDC_TC_GFD,
            "VolumeCondition": THOST_FTDC_VC_AV,
            "MinVolume": 1,
            "ExchangeID": symbol_exchange_map[req.symbol].value
        }
        
        if req.type == OrderType.FAK:
            ctp_req["OrderPriceType"] = THOST_FTDC_OPT_LimitPrice
            ctp_req["TimeCondition"] = THOST_FTDC_TC_IOC
            ctp_req["VolumeCondition"] = THOST_FTDC_VC_AV
        elif req.type == OrderType.FOK:
            ctp_req["OrderPriceType"] = THOST_FTDC_OPT_LimitPrice
            ctp_req["TimeCondition"] = THOST_FTDC_TC_IOC
            ctp_req["VolumeCondition"] = THOST_FTDC_VC_CV            
        
        self.reqid += 1
        self.reqOrderInsert(ctp_req, self.reqid)
        
        orderid = f"{self.frontid}_{self.sessionid}_{self.order_ref}"
        order = req.create_order_data(orderid, self.gateway_name)
        self.gateway.on_order(order)
        
        return order.vt_orderid
    
    def cancel_order(self, req: CancelRequest):
        """
        Cancel existing order.
        """
        frontid, sessionid, order_ref = req.orderid.split("_")
        
        ctp_req = {
            "InstrumentID": req.symbol,
            "Exchange": req.exchange,
            "OrderRef": order_ref,
            "FrontID": int(frontid),
            "SessionID": int(sessionid),
            "ActionFlag": THOST_FTDC_AF_Delete,
            "BrokerID": self.brokerid,
            "InvestorID": self.userid
        }
        
        self.reqid += 1
        self.reqOrderAction(ctp_req, self.reqid)
    
    def query_account(self):
        """
        Query account balance data.
        """
        self.reqid += 1
        self.reqQryTradingAccount({}, self.reqid)
    
    def query_position(self):
        """
        Query position holding data.
        """
        if not symbol_exchange_map:
            return
        
        req = {
            "BrokerID": self.brokerid,
            "InvestorID": self.userid
        }
        
        self.reqid += 1 
        self.reqQryInvestorPosition(req, self.reqid)
    
    def close(self):
        """"""
        if self.connect_status:
            self.exit()


# 增
def round_to(value: float, target: float):
    """
    Round price to price tick value.
    """
    rounded = round(int(round(value / target)) * target, 5)
    return rounded

