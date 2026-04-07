import streamlit as st
import pandas as pd
import sqlite3
import zipfile
import tempfile
import os
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(layout="wide", page_title="天猫新零售数据看板", page_icon="📊")

# ----------------------------- 全国城市坐标（完整版） -----------------------------
CITY_COORDS = {
    '北京': [116.4074, 39.9042], '上海': [121.4737, 31.2304], '天津': [117.1902, 39.1256], '重庆': [106.5044, 29.5582],
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

# ----------------------------- 城市名称清洗与坐标获取 -----------------------------
def get_city_coord(city_name):
    if not city_name or pd.isna(city_name):
        return DEFAULT_COORD
    city_str = str(city_name).strip()
    for suffix in ["市", "区", "县", "省", "自治区", "特别行政区"]:
        city_str = city_str.replace(suffix, "")
    city_str = city_str.strip()
    direct_map = {"北京": "北京", "上海": "上海", "天津": "天津", "重庆": "重庆"}
    city_str = direct_map.get(city_str, city_str)
    coord = CITY_COORDS.get(city_str)
    if coord is None and st.session_state.get("debug_mode", False):
        st.warning(f"未找到城市 '{city_name}' 的坐标，使用北京默认")
    return coord if coord else DEFAULT_COORD

# ----------------------------- 品牌筛选逻辑（修复索引错误） -----------------------------
def apply_brand_filter(df, selected_brands):
    if not selected_brands:
        return df
    cond = pd.Series(False, index=df.index)
    normal_brands = [b for b in selected_brands if b not in ["洗衣机汇总", "美的厨热", "美的冰箱", "美的空调"]]
    if normal_brands:
        cond |= df["品牌"].isin(normal_brands)
    if "洗衣机汇总" in selected_brands:
        cond |= (df["品牌"] == "小天鹅") | ((df["品牌"] == "美的") & (df["品类"] == "洗衣机"))
    if "美的厨热" in selected_brands:
        cond |= (df["品牌"] == "美的") & (df["品类"] == "厨热")
    if "美的冰箱" in selected_brands:
        cond |= (df["品牌"] == "美的") & (df["品类"] == "冰箱")
    if "美的空调" in selected_brands:
        cond |= (df["品牌"] == "美的") & (df["品类"] == "空调")
    return df[cond]

# ----------------------------- 数据加载 -----------------------------
@st.cache_data(ttl=3600)
def load_data():
    if not os.path.exists("data.zip"):
        st.error("❌ 未找到 data.zip 文件，请将数据文件放在应用同目录下")
        st.stop()
    with zipfile.ZipFile("data.zip", "r") as zf:
        db_files = [f for f in zf.namelist() if f.endswith(".db")]
        if not db_files:
            st.error("❌ 压缩包中未找到 .db 文件")
            st.stop()
        with tempfile.NamedTemporaryFile(delete=False, suffix=".db") as tmp:
            with zf.open(db_files[0]) as f:
                tmp.write(f.read())
            tmp_path = tmp.name

    conn = sqlite3.connect(tmp_path)
    try:
        df_main = pd.read_sql("SELECT * FROM 客资明细表", conn)
        df_order = pd.read_sql("SELECT * FROM 订单表", conn)
    except Exception as e:
        st.error(f"数据库读取失败: {e}")
        st.stop()
    finally:
        conn.close()
        os.unlink(tmp_path)

    # 日期处理
    if "获取时间" in df_main.columns:
        df_main["日期"] = pd.to_datetime(df_main["获取时间"], errors="coerce")
    elif "日期" in df_main.columns:
        df_main["日期"] = pd.to_datetime(df_main["日期"], errors="coerce")
    else:
        df_main["日期"] = pd.NaT

    if "日期" in df_order.columns:
        df_order["日期"] = pd.to_datetime(df_order["日期"], errors="coerce")
    else:
        df_order["日期"] = pd.NaT

    if "订单金额" in df_order.columns:
        df_order["订单金额"] = pd.to_numeric(df_order["订单金额"], errors="coerce").fillna(0)
    else:
        df_order["订单金额"] = 0.0

    for df in [df_main, df_order]:
        df["品牌"] = df.get("品牌", df.get("意向品牌", "未知")).fillna("未知")
        df["品类"] = df.get("品类", "未知").fillna("未知")
        df["运营中心"] = df.get("运营中心", df.get("运中", "未知")).fillna("未知")
        df["片区"] = df.get("片区", "未知").fillna("未知")
    if "外呼状态" not in df_main.columns:
        df_main["外呼状态"] = ""
    if "最新跟进状态" not in df_main.columns:
        df_main["最新跟进状态"] = ""
    if "市区" not in df_order.columns:
        df_order["市区"] = ""

    return df_main, df_order

# ----------------------------- 主程序 -----------------------------
df_main, df_order = load_data()
if df_main.empty:
    st.error("客资明细表为空，请检查数据源")
    st.stop()

# 调试模式开关
if "debug_mode" not in st.session_state:
    st.session_state.debug_mode = False
with st.sidebar:
    st.session_state.debug_mode = st.checkbox("🔧 调试模式", value=False)

# 动态生成筛选选项
actual_brands = sorted([b for b in df_main["品牌"].dropna().unique() if b and b != "未知"])
actual_cats = sorted([c for c in df_main["品类"].dropna().unique() if c and c != "未知"])
actual_centers = sorted([c for c in df_main["运营中心"].dropna().unique() if c and c != "未知"])
actual_areas = sorted([a for a in df_main["片区"].dropna().unique() if a and a != "未知"])
brand_options = actual_brands + ["洗衣机汇总", "美的厨热", "美的冰箱", "美的空调"]

st.sidebar.header("🔍 筛选条件")
if not df_main["日期"].isna().all():
    min_date = df_main["日期"].min().date()
    max_date = df_main["日期"].max().date()
else:
    min_date = datetime.today().date()
    max_date = datetime.today().date()
date_range = st.sidebar.date_input("日期范围", [min_date, max_date])

col1_s, col2_s = st.sidebar.columns(2)
with col1_s:
    sel_brand = st.multiselect("品牌", brand_options, default=actual_brands)
    sel_cat = st.multiselect("品类", actual_cats, default=actual_cats)
with col2_s:
    sel_area = st.multiselect("片区", actual_areas, default=actual_areas)
    sel_center = st.multiselect("运营中心", actual_centers, default=actual_centers)

def filter_by_date(df, date_range):
    if "日期" not in df.columns or df["日期"].isna().all():
        return df
    d_start, d_end = date_range
    return df[(df["日期"].dt.date >= d_start) & (df["日期"].dt.date <= d_end)]

df_m = filter_by_date(df_main, date_range)
df_m = apply_brand_filter(df_m, sel_brand)
if sel_cat:
    df_m = df_m[df_m["品类"].isin(sel_cat)]
if sel_center:
    df_m = df_m[df_m["运营中心"].isin(sel_center)]
if sel_area:
    df_m = df_m[df_m["片区"].isin(sel_area)]

df_o = filter_by_date(df_order, date_range)
df_o = apply_brand_filter(df_o, sel_brand)
if sel_cat:
    df_o = df_o[df_o["品类"].isin(sel_cat)]
if sel_center:
    df_o = df_o[df_o["运营中心"].isin(sel_center)]

# 调试信息
if st.session_state.debug_mode:
    with st.sidebar:
        st.markdown("---")
        st.subheader("🔎 诊断信息")
        st.write(f"原始主表行数: {len(df_main)}")
        st.write(f"日期筛选后行数: {len(filter_by_date(df_main, date_range))}")
        st.write(f"最终 df_m 行数: {len(df_m)}")
        if len(df_m) == 0:
            st.error("❌ 无数据，请检查筛选条件与数据中的实际值是否匹配")
            st.write("品牌唯一值:", df_main["品牌"].unique())
            st.write("品类唯一值:", df_main["品类"].unique())

# ----------------------------- 指标卡片 -----------------------------
st.title("🏬 天猫新零售数据看板")
c1, c2, c3, c4 = st.columns(4)
total_leads = len(df_m)
valid_mask = df_m["外呼状态"].isin(["高意向", "低意向", "无需外呼"])
valid_leads = valid_mask.sum()
order_count = len(df_o)
total_amount = df_o["订单金额"].sum() if not df_o.empty else 0.0
total_wan = total_amount / 10000

c1.metric("总客资", f"{total_leads:,}")
c2.metric("有效客资", f"{valid_leads:,}")
c3.metric("成交单量", f"{order_count:,}")
c4.metric("总金额（万元）", f"{total_wan:.2f}")

# ----------------------------- 转化漏斗 -----------------------------
st.header("📉 转化漏斗")
if "最新跟进状态" in df_m.columns and not df_m.empty:
    assigned = df_m[valid_mask & (df_m["最新跟进状态"] != "未分配")].shape[0]
    followed = df_m[valid_mask & (~df_m["最新跟进状态"].isin(["未分配", "待查看", "待联系"]))].shape[0]
else:
    assigned = 0
    followed = 0

funnel_labels = ["总客资", "有效客资", "已分配", "已跟进", "成交"]
funnel_values = [total_leads, valid_leads, assigned, followed, order_count]
fig_funnel = go.Figure(go.Funnel(y=funnel_labels, x=funnel_values))
st.plotly_chart(fig_funnel, use_container_width=True)

# ----------------------------- 转化率趋势（四个率，双轴，分段刻度：0-100%步长10%，100-360%步长20%） -----------------------------
st.header("📈 转化率趋势")
if not df_m.empty and "日期" in df_m and not df_m["日期"].isna().all():
    # 按天聚合
    daily = df_m.groupby(df_m["日期"].dt.date).agg(
        总客资=("品牌", "count"),
        有效客资=("外呼状态", lambda x: x.isin(["高意向", "低意向", "无需外呼"]).sum())
    ).reset_index()
    
    # 已分配、已跟进（基于有效客资）
    valid_df = df_m[valid_mask]
    if not valid_df.empty and "最新跟进状态" in valid_df.columns:
        daily_assign = valid_df.groupby(valid_df["日期"].dt.date).agg(
            已分配=("最新跟进状态", lambda x: (x != "未分配").sum()),
            已跟进=("最新跟进状态", lambda x: (~x.isin(["未分配", "待查看", "待联系"])).sum())
        ).reset_index()
        daily = daily.merge(daily_assign, on="日期", how="left").fillna(0)
    else:
        daily["已分配"] = 0
        daily["已跟进"] = 0
    
    # 成交数
    if not df_o.empty:
        daily_order = df_o.groupby(df_o["日期"].dt.date).size().reset_index(name="成交数")
        daily = daily.merge(daily_order, on="日期", how="left").fillna(0)
    else:
        daily["成交数"] = 0
    
    # 计算四个率
    daily["有效率"] = daily["有效客资"] / daily["总客资"].replace(0, pd.NA)
    daily["分配率"] = daily["已分配"] / daily["有效客资"].replace(0, pd.NA)
    daily["跟进率"] = daily["已跟进"] / daily["已分配"].replace(0, pd.NA)
    daily["转化率"] = daily["成交数"] / daily["有效客资"].replace(0, pd.NA)
    
    # 生成分段刻度：0~1.0 步长0.1 (10%)，1.0~3.6 步长0.2 (20%)
    ticks = []
    # 低区 0 到 1.0
    current = 0.0
    while current <= 1.0 + 1e-9:
        ticks.append(round(current, 6))
        current += 0.1
    # 高区从 1.2 到 3.6
    current = 1.2
    while current <= 3.6 + 1e-9:
        ticks.append(round(current, 6))
        current += 0.2
    # 确保包含 3.6
    if 3.6 not in ticks:
        ticks.append(3.6)
    ticks = sorted(set(ticks))
    ticktext = [f"{int(v*100)}%" for v in ticks]
    
    # 绘制双轴图
    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(x=daily["日期"], y=daily["有效率"], mode='lines+markers', name='有效率', yaxis='y1'))
    fig_trend.add_trace(go.Scatter(x=daily["日期"], y=daily["分配率"], mode='lines+markers', name='分配率', yaxis='y1'))
    fig_trend.add_trace(go.Scatter(x=daily["日期"], y=daily["跟进率"], mode='lines+markers', name='跟进率', yaxis='y1'))
    fig_trend.add_trace(go.Scatter(x=daily["日期"], y=daily["转化率"], mode='lines+markers', name='转化率', yaxis='y2'))
    
    fig_trend.update_layout(
        title="转化率趋势（有效率、分配率、跟进率、转化率）",
        xaxis_title="日期",
        yaxis=dict(
            title="比率",
            side="left",
            tickformat='.0%',
            range=[0, 3.6],
            tickvals=ticks,
            ticktext=ticktext
        ),
        yaxis2=dict(
            title="转化率",
            overlaying='y',
            side="right",
            tickformat='.0%',
            range=[0, 3.6],
            tickvals=ticks,
            ticktext=ticktext
        ),
        legend=dict(x=0.01, y=0.99)
    )
    st.plotly_chart(fig_trend, use_container_width=True)
else:
    st.info("无有效日期数据，无法绘制趋势图")

# ----------------------------- 销售额分布 -----------------------------
st.header("💰 销售额分布")
if df_o.empty:
    st.warning("当前筛选条件下无订单数据，无法展示销售额分布")
else:
    tab1, tab2, tab3 = st.tabs(["品牌", "品类", "运营中心"])
    with tab1:
        brand_sale = df_o.groupby("品牌")["订单金额"].sum().sort_values(ascending=False).head(10).reset_index()
        brand_sale["万元"] = brand_sale["订单金额"] / 10000
        fig1 = px.bar(brand_sale, x="品牌", y="万元", color="万元", title="品牌销售额 Top10")
        st.plotly_chart(fig1, use_container_width=True)
    with tab2:
        cat_sale = df_o.groupby("品类")["订单金额"].sum().reset_index()
        cat_sale["万元"] = cat_sale["订单金额"] / 10000
        fig2 = px.pie(cat_sale, names="品类", values="万元", title="品类销售额占比")
        st.plotly_chart(fig2, use_container_width=True)
    with tab3:
        center_sale = df_o.groupby("运营中心")["订单金额"].sum().reset_index()
        center_sale["万元"] = center_sale["订单金额"] / 10000
        fig3 = px.bar(center_sale, x="运营中心", y="万元", color="万元", title="运营中心销售额")
        st.plotly_chart(fig3, use_container_width=True)

# ----------------------------- 城市销售热力图 -----------------------------
st.header("🗺️ 城市销售热力图")
if not df_o.empty and "市区" in df_o.columns:
    city_sale = df_o.groupby("市区").agg(订单金额=("订单金额", "sum")).reset_index()
    city_sale = city_sale[city_sale["市区"] != ""]
    if not city_sale.empty:
        coords = city_sale["市区"].apply(get_city_coord)
        city_sale[["lon", "lat"]] = pd.DataFrame(coords.tolist(), index=city_sale.index)
        city_sale["万元"] = city_sale["订单金额"] / 10000
        fig_map = px.scatter_mapbox(
            city_sale,
            lat="lat", lon="lon",
            size="万元", color="万元",
            hover_name="市区", hover_data={"万元": ":.2f"},
            mapbox_style="carto-positron",
            zoom=4,
            title="城市销售额热力分布（气泡大小代表销售额）"
        )
        fig_map.update_layout(mapbox=dict(center=dict(lat=35, lon=105)), margin=dict(l=0, r=0, t=30, b=0))
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.info("订单表中无有效的城市信息")
else:
    st.info("暂无城市销售数据")

# ----------------------------- 明细核对 -----------------------------
with st.expander("📄 订单明细（核对总金额）"):
    if not df_o.empty:
        st.dataframe(df_o[["日期", "品牌", "品类", "运营中心", "市区", "订单金额"]], use_container_width=True)
        st.success(f"✅ 订单总金额：{total_amount:,.2f} 元  =  {total_wan:.2f} 万元")
    else:
        st.info("无订单明细")
