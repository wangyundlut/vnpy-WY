# 记录修改了哪些项目
### 修改Ctpgateway的tick推送,变成只有在符合时间的tick才向外推出
### 增加夜盘识别的品种等
### 修改process_timer_event,变成合成1分钟K线的判断依据
### 原访问账户和持仓,变成固定时间(收盘后),和收到成交之后访问
### tick合成1分钟K线,并由gateway推出,推送到eventengine

### 添加BarData字段 起始时间和终止时间
### 修改utility里面1分钟Bar生成逻辑,原VNPY是用闭开来生成,改为开闭
### 增加1分钟K线合成5分钟 15分钟 30分钟 1小时的逻辑

### 由于simnow推送的数据和实盘CTP推送的数据不一样,导致数据对不上
### 上期所平仓是分为平今 和平仓的
### 其余交易所都是平仓的
### 如果在10:15~10:30 之间连接交易所
郑商所和大商所会在10:15 ~ 10:30之间推送tick tick的时间是10:14:59 等




