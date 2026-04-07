import streamlit as st
import pandas as pd
import sqlite3
import zipfile
import tempfile
import os
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(layout="wide", page_title="天猫新零售数据看板", page_icon="📊")

# ----------------------------- 全国城市坐标（完整版，修复直辖市错位） -----------------------------
CITY_COORDS = {
    '北京': [116.4074, 39.9042],
    '上海': [121.4737, 31.2304],
    '天津': [117.1902, 39.1256],
    '重庆': [106.5044, 29.5582],
    '石家庄': [114.5149, 38.0428], '唐山': [118.1827, 39.6307], '秦皇岛': [119.5219, 39.9307], '邯郸': [114.5261, 36.6052],
    '邢台': [114.5093, 37.0671], '保定': [115.4624, 38.8735], '张家口': [114.8722, 40.8286], '承德': [117.9327, 40.9501],
    '沧州': [116.8328, 38.3141], '廊坊': [116.6821, 39.1327], '衡水': [115.7230, 37.7401],
    '太原': [112.5624, 37.8735], '大同': [113.2930, 40.0761], '阳泉': [113.5699, 37.8671], '长治': [113.1269, 36.1919],
    '晋城': [112.8527, 35.4951], '朔州': [112.4305, 39.3281], '晋中': [112.7370, 37.7501], '运城': [110.9901, 35.0271],
    '忻州': [112.7317, 38.4151], '临汾': [111.5172, 36.0851], '吕梁': [111.1324, 37.5191],
    '呼和浩特': [111.7510, 40.8415], '包头': [109.8386, 40.6591], '乌海': [106.8206, 39.6711], '赤峰': [118.9401, 42.2591],
    '通辽': [122.2658, 43.6491], '鄂尔多斯': [109.7814, 39.6111], '呼伦贝尔': [119.7541, 49.2121], '巴彦淖尔': [107.4026, 40.7441],
    '乌兰察布': [113.1258, 41.0601], '兴安盟': [122.5091, 46.0801], '锡林郭勒': [116.1001, 43.9501], '阿拉善': [105.7101, 38.8501],
    '沈阳': [123.4315, 41.8057], '大连': [121.6207, 38.9157], '鞍山': [122.9967, 41.1067], '抚顺': [123.9527, 41.8767],
    '本溪': [123.7677, 41.2967], '丹东': [124.3797, 40.1337], '锦州': [121.1317, 41.1007], '营口': [122.2317, 40.6667],
    '阜新': [121.6497, 42.0207], '辽阳': [123.1727, 41.2767], '盘锦': [122.0697, 41.1197], '铁岭': [123.8497, 42.2907],
    '朝阳': [120.4517, 41.5707], '葫芦岛': [120.8417, 40.7107],
    '长春': [125.3235, 43.8171], '吉林': [126.5515, 43.8371], '四平': [124.3605, 43.1601], '辽源': [125.1305, 42.8901],
    '通化': [125.9305, 41.7301], '白山': [126.4105, 41.9401], '松原': [124.8205, 45.1201], '白城': [122.8305, 45.6201],
    '延边': [129.5005, 42.9001],
    '哈尔滨': [126.5364, 45.8022], '齐齐哈尔': [123.9644, 47.3422], '鸡西': [130.9944, 45.2922], '鹤岗': [130.2844, 47.3322],
    '双鸭山': [131.1744, 46.6422], '大庆': [125.1344, 46.5822], '伊春': [128.9344, 47.7222], '佳木斯': [130.3744, 46.8122],
    '七台河': [130.9844, 45.8022], '牡丹江': [129.6344, 44.5822], '黑河': [127.5344, 50.2322], '绥化': [126.9844, 46.6322],
    '南京': [118.7674, 32.0415], '无锡': [120.3136, 31.4908], '徐州': [117.1387, 34.2633], '常州': [119.9740, 31.8107],
    '苏州': [120.5853, 31.2989], '南通': [120.8646, 32.0167], '连云港': [119.1629, 34.5927], '淮安': [119.0212, 33.6268],
    '盐城': [120.1399, 33.3776], '扬州': [119.4256, 32.3937], '镇江': [119.4529, 32.2044], '泰州': [119.9078, 32.4588],
    '杭州': [120.1551, 30.2741], '宁波': [121.5438, 29.8683], '温州': [120.6994, 27.9949], '嘉兴': [120.7508, 30.7536],
    '湖州': [120.0997, 30.8703], '绍兴': [120.5818, 30.0082], '金华': [119.6407, 29.0895], '衢州': [118.8803, 28.9703],
    '舟山': [122.2072, 29.9856], '台州': [121.4208, 28.6563], '丽水': [119.9152, 28.4527],
    '合肥': [117.2272, 31.8206], '芜湖': [118.3830, 31.3533], '蚌埠': [117.3583, 32.9183], '淮南': [116.9985, 32.6476],
    '福州': [119.2965, 26.0745], '厦门': [118.0895, 24.4798], '莆田': [119.0025, 25.4405], '三明': [117.6405, 26.2505],
    '南昌': [115.8582, 28.6820], '济南': [117.0009, 36.6758], '青岛': [120.3826, 36.0671], '郑州': [113.6254, 34.7466],
    '武汉': [114.3055, 30.5931], '长沙': [112.9388, 28.2282], '广州': [113.2644, 23.1291], '深圳': [114.0579, 22.5431],
    '南宁': [108.3661, 22.8176], '海口': [110.1999, 20.0440], '成都': [104.0668, 30.5728], '贵阳': [106.6302, 26.6477],
    '昆明': [102.8329, 24.8801], '拉萨': [91.1409, 29.6565], '西安': [108.9402, 34.3416], '兰州': [103.8343, 36.0611],
    '西宁': [101.7782, 36.6232], '银川': [106.2309, 38.4872], '乌鲁木齐': [87.6168, 43.8256]
}
DEFAULT_COORD = [116.4074, 39.9042]

# ----------------------------- 城市名称清洗（修复直辖市） -----------------------------
def get_city_coord(city_name):
    if not city_name or pd.isna(city_name):
        return DEFAULT_COORD
    city_str = str(city_name).strip()
    for s in ["市", "区", "县", "省", "自治区", "特别行政区"]:
        city_str = city_str.replace(s, "")
    city_str = city_str.strip()
    direct_map = {"北京": "北京", "上海": "上海", "天津": "天津", "重庆": "重庆"}
    city_str = direct_map.get(city_str, city_str)
    return CITY_COORDS.get(city_str, DEFAULT_COORD)

# ----------------------------- 数据加载 -----------------------------
@st.cache_data(ttl=3600)
def load_data():
    with zipfile.ZipFile("data.zip", "r") as zf:
        db_files = [f for f in zf.namelist() if f.endswith(".db")]
        if not db_files:
            st.error("未找到.db文件")
            st.stop()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp:
            with zf.open(db_files[0]) as f:
                tmp.write(f.read())
            tmp_path = tmp.name

    conn = sqlite3.connect(tmp_path)
    df_main = pd.read_sql("SELECT * FROM 客资明细表", conn)
    df_order = pd.read_sql("SELECT * FROM 订单表", conn)
    conn.close()
    os.unlink(tmp_path)

    # 日期处理
    if "获取时间" in df_main.columns:
        df_main["日期"] = pd.to_datetime(df_main["获取时间"], errors="coerce")
    if "日期" in df_order.columns:
        df_order["日期"] = pd.to_datetime(df_order["日期"], errors="coerce")

    # 金额清洗（保证总金额精准）
    df_order["订单金额"] = pd.to_numeric(df_order["订单金额"], errors="coerce").fillna(0)

    # 统一字段
    for df in [df_main, df_order]:
        df["品牌"] = df.get("品牌", df.get("意向品牌", "未知")).fillna("未知")
        df["品类"] = df.get("品类", "未知").fillna("未知")
        df["运营中心"] = df.get("运营中心", df.get("运中", "未知")).fillna("未知")
        df["片区"] = df.get("片区", "未知").fillna("未知")

    df_main["外呼状态"] = df_main.get("外呼状态", "")
    df_main["最新跟进状态"] = df_main.get("最新跟进状态", "")
    df_order["市区"] = df_order.get("市区", "")

    return df_main, df_order

df_main, df_order = load_data()

# ----------------------------- 筛选 -----------------------------
st.sidebar.header("🔍 筛选条件")
min_date = df_main["日期"].min().date() if "日期" in df_main and not df_main["日期"].isna().all() else datetime.today().date()
max_date = df_main["日期"].max().date() if "日期" in df_main and not df_main["日期"].isna().all() else datetime.today().date()
date_range = st.sidebar.date_input("日期范围", [min_date, max_date])

brand_list = ["美的", "东芝", "小天鹅", "COLMO", "美的厨热", "美的冰箱", "美的空调", "洗衣机汇总"]
cat_list = sorted([x for x in df_main["品类"].dropna().unique() if x])
center_list = sorted([x for x in df_main["运营中心"].dropna().unique() if x])
area_list = sorted([x for x in df_main["片区"].dropna().unique() if x])

col1_s, col2_s = st.sidebar.columns(2)
with col1_s:
    sel_brand = st.multiselect("品牌", brand_list, default=brand_list)
    sel_cat = st.multiselect("品类", cat_list, default=cat_list)
with col2_s:
    sel_area = st.multiselect("片区", area_list, default=area_list)
    sel_center = st.multiselect("运营中心", center_list, default=center_list)

# ----------------------------- 品牌过滤逻辑 -----------------------------
def brand_filter(df, brands):
    if not brands:
        return df
    res = []
    for _, row in df.iterrows():
        b = row["品牌"]
        c = row["品类"]
        ok = False
        for s in brands:
            if s == "美的" and b == "美的": ok = True
            elif s == "东芝" and b == "东芝": ok = True
            elif s == "小天鹅" and b == "小天鹅": ok = True
            elif s == "COLMO" and b == "COLMO": ok = True
            elif s == "美的厨热" and b == "美的" and c == "厨热": ok = True
            elif s == "美的冰箱" and b == "美的" and c == "冰箱": ok = True
            elif s == "美的空调" and b == "美的" and c == "空调": ok = True
            elif s == "洗衣机汇总" and (b == "小天鹅" or (b == "美的" and c == "洗衣机")): ok = True
        if ok:
            res.append(row)
    return pd.DataFrame(res)

# 统一过滤
def df_filter(df, date_range):
    if "日期" not in df.columns:
        return df
    d_start, d_end = date_range
    return df[(df["日期"].dt.date >= d_start) & (df["日期"].dt.date <= d_end)]

df_m = df_filter(df_main, date_range)
df_m = brand_filter(df_m, sel_brand)
df_m = df_m[df_m["品类"].isin(sel_cat)]
df_m = df_m[df_m["运营中心"].isin(sel_center)]
df_m = df_m[df_m["片区"].isin(sel_area)]

df_o = df_filter(df_order, date_range)
df_o = brand_filter(df_o, sel_brand)
df_o = df_o[df_o["品类"].isin(sel_cat)]
df_o = df_o[df_o["运营中心"].isin(sel_center)]

# ----------------------------- 指标卡片 -----------------------------
st.title("🏬 天猫新零售数据看板")
c1, c2, c3, c4 = st.columns(4)
total_leads = len(df_m)
valid_leads = len(df_m[df_m["外呼状态"].isin(["高意向", "低意向", "无需外呼"])])
order_count = len(df_o)
total_amount = df_o["订单金额"].sum()
total_wan = total_amount / 10000

c1.metric("总客资", f"{total_leads:,}")
c2.metric("有效客资", f"{valid_leads:,}")
c3.metric("成交单量", f"{order_count:,}")
c4.metric("总金额（万元）", f"{total_wan:.2f}")

# ----------------------------- 漏斗图 -----------------------------
st.header("📉 转化漏斗")
funnel_labels = ["总客资", "有效客资", "已分配", "已跟进", "成交"]
funnel_values = [
    len(df_m),
    valid_leads,
    len(df_m[(df_m["外呼状态"].isin(["高意向", "低意向", "无需外呼"])) & (df_m["最新跟进状态"] != "未分配")]) if "最新跟进状态" in df_m else 0,
    len(df_m[(df_m["外呼状态"].isin(["高意向", "低意向", "无需外呼"])) & (~df_m["最新跟进状态"].isin(["未分配", "待查看", "待联系"]))]) if "最新跟进状态" in df_m else 0,
    len(df_o)
]
st.plotly_chart(go.Figure(go.Funnel(y=funnel_labels, x=funnel_values)), use_container_width=True)

# ----------------------------- 趋势图 -----------------------------
st.header("📈 转化率趋势")
if not df_m.empty and "日期" in df_m:
    daily = df_m.groupby(df_m["日期"].dt.date).agg(
        总客资=("客户ID", "count"),
        有效客资=("外呼状态", lambda x: x.isin(["高意向", "低意向", "无需外呼"]).sum())
    ).reset_index()
    daily_order = df_o.groupby(df_o["日期"].dt.date).size().reset_index(name="成交数")
    daily = daily.merge(daily_order, on="日期", how="left").fillna(0)
    daily["成交率"] = daily["成交数"] / daily["有效客资"].replace(0, pd.NA)
    fig = px.line(daily, x="日期", y=["有效客资", "成交数", "成交率"], markers=True)
    st.plotly_chart(fig, use_container_width=True)

# ----------------------------- 销售额分布（热力图/分布图） -----------------------------
st.header("💰 销售额分布")
tab1, tab2, tab3 = st.tabs(["品牌", "品类", "运营中心"])
with tab1:
    if not df_o.empty:
        top_brand = df_o.groupby("品牌")["订单金额"].sum().sort_values(ascending=False).head(10).reset_index()
        top_brand["万元"] = top_brand["订单金额"] / 10000
        st.plotly_chart(px.bar(top_brand, x="品牌", y="万元", color="万元"), use_container_width=True)
with tab2:
    if not df_o.empty:
        cat_sale = df_o.groupby("品类")["订单金额"].sum().reset_index()
        cat_sale["万元"] = cat_sale["订单金额"] / 10000
        st.plotly_chart(px.pie(cat_sale, names="品类", values="万元"), use_container_width=True)
with tab3:
    if not df_o.empty:
        center_sale = df_o.groupby("运营中心")["订单金额"].sum().reset_index()
        center_sale["万元"] = center_sale["订单金额"] / 10000
        st.plotly_chart(px.bar(center_sale, x="运营中心", y="万元", color="万元"), use_container_width=True)

# ----------------------------- 地图热力图 -----------------------------
st.header("🗺️ 城市销售热力图")
if not df_o.empty and "市区" in df_o.columns:
    city_sale = df_o.groupby("市区").agg(订单金额=("订单金额", "sum")).reset_index()
    city_sale = city_sale[city_sale["市区"] != ""]
    coords = city_sale["市区"].apply(get_city_coord)
    city_sale[["lon", "lat"]] = pd.DataFrame(coords.tolist(), index=city_sale.index)
    city_sale["万元"] = city_sale["订单金额"] / 10000
    fig_map = px.scatter_mapbox(
        city_sale,
        lat="lat", lon="lon",
        size="万元", color="万元",
        hover_name="市区",
        mapbox_style="carto-positron",
        zoom=4
    )
    st.plotly_chart(fig_map, use_container_width=True)
else:
    st.info("暂无城市销售数据")

# ----------------------------- 明细核对 -----------------------------
with st.expander("📄 订单明细（核对总金额）"):
    st.dataframe(df_o[["日期", "品牌", "品类", "运营中心", "市区", "订单金额"]], use_container_width=True)
    st.success(f"✅ 总金额（元）：{total_amount:.2f}  |  万元：{total_wan:.2f}")
