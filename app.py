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

# ----------------------------- 内置城市经纬度（主要城市） -----------------------------
CITY_COORDS = {
    '北京': [116.4074, 39.9042], '上海': [121.4737, 31.2304], '广州': [113.2644, 23.1291],
    '深圳': [114.0579, 22.5431], '杭州': [120.1551, 30.2741], '成都': [104.0668, 30.5728],
    '武汉': [114.3055, 30.5931], '西安': [108.9402, 34.3416], '南京': [118.7674, 32.0415],
    '重庆': [106.5044, 29.5582], '天津': [117.1902, 39.1256], '苏州': [120.5853, 31.2989],
    '长沙': [112.9388, 28.2282], '郑州': [113.6254, 34.7466], '沈阳': [123.4315, 41.8057],
    '青岛': [120.3826, 36.0671], '宁波': [121.5438, 29.8683], '无锡': [120.3136, 31.4908],
    '佛山': [113.1224, 23.0215], '东莞': [113.7518, 23.0205], '合肥': [117.2272, 31.8206],
    '福州': [119.2965, 26.0745], '厦门': [118.0895, 24.4798], '哈尔滨': [126.5364, 45.8022],
    '长春': [125.3235, 43.8171], '石家庄': [114.5149, 38.0428], '太原': [112.5624, 37.8735],
    '济南': [117.0009, 36.6758], '昆明': [102.8329, 24.8801], '南宁': [108.3661, 22.8176],
    '南昌': [115.8582, 28.6820], '贵阳': [106.6302, 26.6477], '兰州': [103.8343, 36.0611],
    '乌鲁木齐': [87.6168, 43.8256], '呼和浩特': [111.7510, 40.8415], '银川': [106.2309, 38.4872],
    '西宁': [101.7782, 36.6232], '拉萨': [91.1409, 29.6565], '海口': [110.1999, 20.0440],
}
DEFAULT_COORD = [116.4074, 39.9042]  # 北京

def get_city_coord(city_name):
    if not city_name or pd.isna(city_name):
        return DEFAULT_COORD
    return CITY_COORDS.get(city_name, DEFAULT_COORD)

# ----------------------------- 数据加载 -----------------------------
@st.cache_data(ttl=86400)
def load_data():
    with zipfile.ZipFile('data.zip', 'r') as z:
        db_files = [f for f in z.namelist() if f.endswith('.db')]
        if not db_files:
            st.error("data.zip 中没有找到 .db 文件")
            st.stop()
        with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
            with z.open(db_files[0]) as source:
                tmp.write(source.read())
            tmp_path = tmp.name
    conn = sqlite3.connect(tmp_path)
    df_main = pd.read_sql("SELECT * FROM 客资明细表", conn)
    df_order = pd.read_sql("SELECT * FROM 订单表", conn)
    conn.close()
    os.unlink(tmp_path)

    # 重命名列（忽略不存在的列）
    df_main.rename(columns={
        '获取时间': '日期',
        '意向品牌': '品牌',
        '运营中心': '运营中心',
        '片区': '片区',
        '品类': '品类',
        '外呼状态': '外呼状态',
        '最新跟进状态': '最新跟进状态',
    }, inplace=True, errors='ignore')
    df_order.rename(columns={
        '日期': '日期',
        '订单金额': '订单金额',
        '品牌': '品牌',
        '品类': '品类',
        '运中': '运营中心'
    }, inplace=True, errors='ignore')

    # 确保日期列存在
    if '日期' not in df_main.columns:
        df_main['日期'] = pd.NaT
    if '日期' not in df_order.columns:
        df_order['日期'] = pd.NaT

    df_main['日期'] = pd.to_datetime(df_main['日期'], errors='coerce')
    df_order['日期'] = pd.to_datetime(df_order['日期'], errors='coerce')
    df_order['订单金额'] = pd.to_numeric(df_order['订单金额'], errors='coerce').fillna(0)

    # 填充缺失值
    for col in ['品牌', '品类', '运营中心', '片区']:
        if col in df_main.columns:
            df_main[col] = df_main[col].fillna('未知')
        if col in df_order.columns:
            df_order[col] = df_order[col].fillna('未知')
    for col in ['外呼状态', '最新跟进状态']:
        if col not in df_main.columns:
            df_main[col] = ''
    return df_main, df_order

df_main, df_order = load_data()

# 在侧边栏显示数据概览
st.sidebar.subheader("📋 数据概览")
st.sidebar.write(f"客资表行数: {len(df_main)}")
st.sidebar.write(f"订单表行数: {len(df_order)}")
if len(df_main) == 0:
    st.sidebar.warning("客资表无数据，请检查data.zip内容")
if len(df_order) == 0:
    st.sidebar.warning("订单表无数据，请检查data.zip内容")

# 预览数据（前5行）
with st.sidebar.expander("预览客资表（前5行）"):
    st.dataframe(df_main.head())
with st.sidebar.expander("预览订单表（前5行）"):
    st.dataframe(df_order.head())

# ----------------------------- 工具函数 -----------------------------
def get_unique_sorted(series):
    if series.empty:
        return []
    return sorted(series.dropna().unique())

# ====================== 品牌筛选 ======================
def filter_by_brand(df, brand_selections):
    if df.empty or not brand_selections:
        return df.copy()
    if '品牌' not in df.columns:
        return df.copy()
    df = df.reset_index(drop=True).copy()
    has_cat = '品类' in df.columns
    df['品牌'] = df['品牌'].fillna('未知')
    if has_cat:
        df['品类'] = df['品类'].fillna('未知')
    mask = [False] * len(df)
    for idx, row in enumerate(df.itertuples(index=False)):
        brand = getattr(row, '品牌')
        cat = getattr(row, '品类') if has_cat else None
        for b in brand_selections:
            if b == '美的' and brand == '美的':
                mask[idx] = True
                break
            elif b == '东芝' and brand == '东芝':
                mask[idx] = True
                break
            elif b == '小天鹅' and brand == '小天鹅':
                mask[idx] = True
                break
            elif b == 'COLMO' and brand == 'COLMO':
                mask[idx] = True
                break
            elif b == '美的厨热' and has_cat and brand == '美的' and cat == '厨热':
                mask[idx] = True
                break
            elif b == '美的冰箱' and has_cat and brand == '美的' and cat == '冰箱':
                mask[idx] = True
                break
            elif b == '美的空调' and has_cat and brand == '美的' and cat == '空调':
                mask[idx] = True
                break
            elif b == '洗衣机汇总' and (brand == '小天鹅' or (has_cat and brand == '美的' and cat == '洗衣机')):
                mask[idx] = True
                break
    return df[mask].copy()

# ----------------------------- 侧边栏筛选 -----------------------------
st.sidebar.header("🔍 数据筛选")

# 日期范围（智能默认值）
if not df_main['日期'].isna().all():
    min_date = df_main['日期'].min().date()
    max_date = df_main['日期'].max().date()
else:
    min_date = datetime.today().date()
    max_date = datetime.today().date()
date_range = st.sidebar.date_input("日期范围", [min_date, max_date])

brand_options = ['美的', '东芝', '小天鹅', 'COLMO', '美的厨热', '美的冰箱', '美的空调', '洗衣机汇总']
selected_brands = st.sidebar.multiselect("品牌", brand_options, default=brand_options)

if '品类' in df_main.columns:
    category_options = get_unique_sorted(df_main['品类'])
else:
    category_options = []
selected_categories = st.sidebar.multiselect("品类", category_options, default=category_options)

if '片区' in df_main.columns:
    region_options = get_unique_sorted(df_main['片区'])
else:
    region_options = []
selected_regions = st.sidebar.multiselect("片区", region_options, default=region_options)

if '运营中心' in df_main.columns:
    center_options = get_unique_sorted(df_main['运营中心'])
else:
    center_options = []
selected_centers = st.sidebar.multiselect("运营中心", center_options, default=center_options)

# ----------------------------- 筛选函数 -----------------------------
def filter_main(df, date_range, categories, regions, centers):
    if len(date_range) == 2:
        s, e = date_range
        df = df[(df['日期'].dt.date >= s) & (df['日期'].dt.date <= e)]
    if categories and '品类' in df.columns:
        df = df[df['品类'].isin(categories)]
    if regions and '片区' in df.columns:
        df = df[df['片区'].isin(regions)]
    if centers and '运营中心' in df.columns:
        df = df[df['运营中心'].isin(centers)]
    return df

def filter_order(df, date_range, categories, centers):
    if len(date_range) == 2:
        s, e = date_range
        df = df[(df['日期'].dt.date >= s) & (df['日期'].dt.date <= e)]
    if categories and '品类' in df.columns:
        df = df[df['品类'].isin(categories)]
    if centers and '运营中心' in df.columns:
        df = df[df['运营中心'].isin(centers)]
    return df

df_main_filtered = filter_main(df_main, date_range, selected_categories, selected_regions, selected_centers)
df_main_filtered = filter_by_brand(df_main_filtered, selected_brands)

df_order_filtered = filter_order(df_order, date_range, selected_categories, selected_centers)
df_order_filtered = filter_by_brand(df_order_filtered, selected_brands)

# 显示筛选后行数
st.sidebar.markdown("---")
st.sidebar.write(f"筛选后客资数: {len(df_main_filtered)}")
st.sidebar.write(f"筛选后订单数: {len(df_order_filtered)}")

# ----------------------------- 指标卡片 -----------------------------
st.title("🏬 天猫新零售数据看板")
col1, col2, col3, col4 = st.columns(4)

total_leads = len(df_main_filtered)
valid_leads = len(df_main_filtered[df_main_filtered['外呼状态'].isin(['高意向','低意向','无需外呼'])]) if '外呼状态' in df_main_filtered.columns else 0
total_orders = len(df_order_filtered)
total_amount = df_order_filtered['订单金额'].sum() if not df_order_filtered.empty else 0

col1.metric("总客资数", f"{total_leads:,}")
col2.metric("有效客资数", f"{valid_leads:,}")
col3.metric("成交数", f"{total_orders:,}")
col4.metric("总金额", f"{total_amount:,.0f} 元")

# 如果数据为空，显示提示并停止后续图表
if total_leads == 0 and total_orders == 0:
    st.warning("当前筛选条件下没有数据，请调整筛选条件或检查数据源。")
    st.stop()

# ----------------------------- 环比 -----------------------------
def calc_change(c, p):
    return (c-p)/p if p != 0 else None

today = datetime.today().date()
yesterday = today - timedelta(days=1)
amt_today = df_order_filtered[df_order_filtered['日期'].dt.date == today]['订单金额'].sum() if not df_order_filtered.empty else 0
amt_yes = df_order_filtered[df_order_filtered['日期'].dt.date == yesterday]['订单金额'].sum() if not df_order_filtered.empty else 0
dc = calc_change(amt_today, amt_yes)
if dc is not None:
    st.sidebar.metric("日环比", f"{dc:.1%}")

first_cur = datetime(today.year, today.month, 1).date()
if today.month == 1:
    first_prev = datetime(today.year-1, 12, 1).date()
else:
    first_prev = datetime(today.year, today.month-1, 1).date()
amt_cur_m = df_order_filtered[df_order_filtered['日期'].dt.date >= first_cur]['订单金额'].sum() if not df_order_filtered.empty else 0
amt_pre_m = df_order_filtered[(df_order_filtered['日期'].dt.date >= first_prev) & (df_order_filtered['日期'].dt.date < first_cur)]['订单金额'].sum() if not df_order_filtered.empty else 0
mc = calc_change(amt_cur_m, amt_pre_m)
if mc is not None:
    st.sidebar.metric("月环比", f"{mc:.1%}")

# ----------------------------- 转化漏斗 -----------------------------
st.header("📉 转化漏斗")

def funnel(main, order):
    t = len(main)
    if '外呼状态' not in main.columns:
        v = a = f = 0
    else:
        valid_mask = main['外呼状态'].isin(['高意向','低意向','无需外呼'])
        v = valid_mask.sum()
        if '最新跟进状态' not in main.columns:
            a = f = 0
        else:
            allocated_mask = valid_mask & (~main['最新跟进状态'].isin(['未分配']))
            a = allocated_mask.sum()
            followed_mask = allocated_mask & (~main['最新跟进状态'].isin(['未分配','待查看','待联系']))
            f = followed_mask.sum()
    o = len(order)
    return [t, v, a, f, o]

stages = ["总客资", "有效客资", "分配数", "跟进数", "成交数"]
vals = funnel(df_main_filtered, df_order_filtered)
fig_funnel = go.Figure(go.Funnel(y=stages, x=vals, textinfo="value+percent initial"))
st.plotly_chart(fig_funnel, use_container_width=True)

# ----------------------------- 转化率趋势 -----------------------------
st.header("📈 转化率趋势")
if not df_main_filtered.empty and '日期' in df_main_filtered.columns:
    # 按天分组
    daily = df_main_filtered.groupby(df_main_filtered['日期'].dt.date).apply(
        lambda x: pd.Series({
            '总客资': len(x),
            '有效客资': len(x[x['外呼状态'].isin(['高意向','低意向','无需外呼'])]) if '外呼状态' in x.columns else 0,
            '分配数': len(x[(x['外呼状态'].isin(['高意向','低意向','无需外呼'])) & (~x['最新跟进状态'].isin(['未分配']))]) if all(c in x.columns for c in ['外呼状态','最新跟进状态']) else 0,
            '跟进数': len(x[(x['外呼状态'].isin(['高意向','低意向','无需外呼'])) & (~x['最新跟进状态'].isin(['未分配','待查看','待联系']))]) if all(c in x.columns for c in ['外呼状态','最新跟进状态']) else 0
        })
    ).reset_index()
    d_ord = df_order_filtered.groupby(df_order_filtered['日期'].dt.date).size().reset_index(name='成交数') if not df_order_filtered.empty else pd.DataFrame(columns=['日期','成交数'])
    daily = daily.merge(d_ord, on='日期', how='left').fillna(0)
    daily['有效率'] = daily['有效客资'] / daily['总客资'].replace(0, pd.NA)
    daily['分配率'] = daily['分配数'] / daily['有效客资'].replace(0, pd.NA)
    daily['跟进率'] = daily['跟进数'] / daily['分配数'].replace(0, pd.NA)
    daily['成交率'] = daily['成交数'] / daily['跟进数'].replace(0, pd.NA)

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(x=daily['日期'], y=daily['有效率'], name='有效率', mode='lines+markers'))
    fig_trend.add_trace(go.Scatter(x=daily['日期'], y=daily['分配率'], name='分配率', mode='lines+markers'))
    fig_trend.add_trace(go.Scatter(x=daily['日期'], y=daily['跟进率'], name='跟进率', mode='lines+markers'))
    fig_trend.add_trace(go.Scatter(x=daily['日期'], y=daily['成交率'], name='成交率', mode='lines+markers'))
    fig_trend.update_layout(yaxis_tickformat=".0%", yaxis_title="比率", xaxis_title="日期")
    st.plotly_chart(fig_trend, use_container_width=True)
else:
    st.info("客资数据为空或缺少日期列，无法绘制趋势图。")

# ----------------------------- 各品牌销售额 -----------------------------
st.header("📊 各品牌销售额")
if not df_order_filtered.empty and '品牌' in df_order_filtered.columns:
    bs = df_order_filtered.groupby('品牌')['订单金额'].sum().sort_values(ascending=False).head(10).reset_index()
    st.plotly_chart(px.bar(bs, x='品牌', y='订单金额', color='订单金额', title="Top10 品牌销售额"), use_container_width=True)
else:
    st.info("订单数据为空或缺少品牌列。")

st.subheader("销售额分布")
t1, t2, t3 = st.tabs(["品类", "片区", "运营中心"])
with t1:
    if not df_order_filtered.empty and '品类' in df_order_filtered.columns:
        c = df_order_filtered.groupby('品类')['订单金额'].sum().reset_index()
        st.plotly_chart(px.bar(c, x='品类', y='订单金额', title="品类销售额"), use_container_width=True)
    else:
        st.info("无品类数据")
with t2:
    if not df_order_filtered.empty and '片区' in df_order_filtered.columns:
        r = df_order_filtered.groupby('片区')['订单金额'].sum().reset_index()
        st.plotly_chart(px.bar(r, x='片区', y='订单金额', title="片区销售额"), use_container_width=True)
    else:
        st.info("无片区数据")
with t3:
    if not df_order_filtered.empty and '运营中心' in df_order_filtered.columns:
        ce = df_order_filtered.groupby('运营中心')['订单金额'].sum().sort_values(ascending=False).head(15).reset_index()
        st.plotly_chart(px.bar(ce, x='运营中心', y='订单金额', title="运营中心销售额"), use_container_width=True)
    else:
        st.info("无运营中心数据")

# ----------------------------- 中国地图热力图 -----------------------------
st.header("🗺️ 中国地图 - 市区销售额热力图")
if not df_order_filtered.empty and '市区' in df_order_filtered.columns:
    # 过滤掉市区为空的行
    city_data = df_order_filtered[df_order_filtered['市区'].notna() & (df_order_filtered['市区'] != '')]
    if city_data.empty:
        st.warning("订单表中「市区」字段全部为空，无法绘制地图。")
    else:
        city_amount = city_data.groupby('市区')['订单金额'].sum().reset_index()
        city_amount = city_amount.sort_values('订单金额', ascending=False)
        # 添加经纬度
        city_amount['经度'] = city_amount['市区'].apply(lambda x: get_city_coord(x)[0])
        city_amount['纬度'] = city_amount['市区'].apply(lambda x: get_city_coord(x)[1])
        # 去重（理论上已经groupby，不会有重复市区）
        unique_cities = city_amount.drop_duplicates(subset=['市区'])
        
        # 限制显示前100个城市（避免性能问题）
        if len(unique_cities) > 100:
            unique_cities = unique_cities.head(100)
        
        fig_map = px.scatter_mapbox(
            unique_cities,
            lat='纬度',
            lon='经度',
            size='订单金额',
            color='订单金额',
            color_continuous_scale='YlOrRd',
            size_max=40,
            zoom=3,
            mapbox_style='open-street-map',
            text='市区',
            hover_name='市区',
            hover_data={'订单金额': ':,.0f', '纬度': False, '经度': False},
            title='全国各市订单金额分布（气泡大小/颜色代表金额）'
        )
        fig_map.update_layout(margin=dict(l=0, r=0, t=40, b=0), height=600)
        st.plotly_chart(fig_map, use_container_width=True)
        
        with st.expander("查看详细数据"):
            st.dataframe(unique_cities[['市区', '订单金额']].style.format({'订单金额': '{:,.0f}'}))
else:
    st.warning("订单表中没有找到「市区」字段，无法绘制地图。")

st.caption("数据实时更新 | 有效客资：高/低意向/无需外呼")