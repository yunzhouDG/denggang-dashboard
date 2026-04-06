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

    df_main['日期'] = pd.to_datetime(df_main['日期'], errors='coerce')
    df_order['日期'] = pd.to_datetime(df_order['日期'], errors='coerce')
    df_order['订单金额'] = pd.to_numeric(df_order['订单金额'], errors='coerce').fillna(0)

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

# ----------------------------- 工具函数 -----------------------------
def get_unique_sorted(series):
    return sorted(series.dropna().unique())

# ====================== 安全的品牌筛选（防御性检查） ======================
def filter_by_brand(df, brand_selections):
    if df.empty or not brand_selections:
        return df.copy()
    if '品牌' not in df.columns:
        # 没有品牌列，无法筛选，返回原数据
        return df.copy()
    
    df = df.reset_index(drop=True).copy()
    has_cat = '品类' in df.columns
    
    # 提取列为普通列表，避免 pandas 索引
    brands = df['品牌'].fillna('未知').tolist()
    cats = df['品类'].fillna('未知').tolist() if has_cat else None
    
    # 构建布尔掩码
    mask = [False] * len(df)
    for i, brand in enumerate(brands):
        for b in brand_selections:
            if b == '美的' and brand == '美的':
                mask[i] = True
                break
            elif b == '东芝' and brand == '东芝':
                mask[i] = True
                break
            elif b == '小天鹅' and brand == '小天鹅':
                mask[i] = True
                break
            elif b == 'COLMO' and brand == 'COLMO':
                mask[i] = True
                break
            elif b == '美的厨热' and has_cat and brand == '美的' and cats[i] == '厨热':
                mask[i] = True
                break
            elif b == '美的冰箱' and has_cat and brand == '美的' and cats[i] == '冰箱':
                mask[i] = True
                break
            elif b == '美的空调' and has_cat and brand == '美的' and cats[i] == '空调':
                mask[i] = True
                break
            elif b == '洗衣机汇总' and (
                brand == '小天鹅' or (has_cat and brand == '美的' and cats[i] == '洗衣机')
            ):
                mask[i] = True
                break
    
    return df[mask].copy()

# ----------------------------- 侧边栏 -----------------------------
st.sidebar.header("🔍 数据筛选")

min_date = df_main['日期'].min().date() if not df_main['日期'].isna().all() else datetime.today().date()
max_date = df_main['日期'].max().date() if not df_main['日期'].isna().all() else datetime.today().date()
date_range = st.sidebar.date_input("日期范围", [min_date, max_date])

brand_options = ['美的', '东芝', '小天鹅', 'COLMO', '美的厨热', '美的冰箱', '美的空调', '洗衣机汇总']
selected_brands = st.sidebar.multiselect("品牌", brand_options, default=brand_options)

category_options = get_unique_sorted(df_main['品类'])
selected_categories = st.sidebar.multiselect("品类", category_options, default=category_options)

region_options = get_unique_sorted(df_main['片区'])
selected_regions = st.sidebar.multiselect("片区", region_options, default=region_options)

center_options = get_unique_sorted(df_main['运营中心'])
selected_centers = st.sidebar.multiselect("运营中心", center_options, default=center_options)

# ----------------------------- 筛选 -----------------------------
def filter_main(df, date_range, categories, regions, centers):
    if len(date_range) == 2:
        s, e = date_range
        df = df[(df['日期'].dt.date >= s) & (df['日期'].dt.date <= e)]
    if categories:
        df = df[df['品类'].isin(categories)]
    if regions:
        df = df[df['片区'].isin(regions)]
    if centers:
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

# ----------------------------- 指标 -----------------------------
st.title("🏬 天猫新零售数据看板")
col1, col2, col3, col4 = st.columns(4)

total_leads = len(df_main_filtered)
valid_leads = len(df_main_filtered[df_main_filtered['外呼状态'].isin(['高意向','低意向','无需外呼'])])
total_orders = len(df_order_filtered)
total_amount = df_order_filtered['订单金额'].sum()

col1.metric("总客资数", f"{total_leads:,}")
col2.metric("有效客资数", f"{valid_leads:,}")
col3.metric("成交数", f"{total_orders:,}")
col4.metric("总金额", f"{total_amount:,.0f} 元")

# ----------------------------- 环比 -----------------------------
def calc_change(c, p):
    return (c-p)/p if p !=0 else None

today = datetime.today().date()
yesterday = today - timedelta(days=1)
amt_today = df_order_filtered[df_order_filtered['日期'].dt.date == today]['订单金额'].sum()
amt_yes = df_order_filtered[df_order_filtered['日期'].dt.date == yesterday]['订单金额'].sum()
dc = calc_change(amt_today, amt_yes)
if dc is not None:
    st.sidebar.metric("日环比", f"{dc:.1%}")

first_cur = datetime(today.year, today.month, 1).date()
first_prev = datetime(today.year-1,12,1).date() if today.month==1 else datetime(today.year, today.month-1,1).date()
amt_cur_m = df_order_filtered[df_order_filtered['日期'].dt.date >= first_cur]['订单金额'].sum()
amt_pre_m = df_order_filtered[(df_order_filtered['日期'].dt.date >= first_prev) & (df_order_filtered['日期'].dt.date < first_cur)]['订单金额'].sum()
mc = calc_change(amt_cur_m, amt_pre_m)
if mc is not None:
    st.sidebar.metric("月环比", f"{mc:.1%}")

# ----------------------------- 漏斗 -----------------------------
st.header("📉 转化漏斗")
def funnel(main, order):
    t = len(main)
    v = len(main[main['外呼状态'].isin(['高意向','低意向','无需外呼'])])
    a = len(main[(main['外呼状态'].isin(['高意向','低意向','无需外呼'])) & (~main['最新跟进状态'].isin(['未分配']))])
    f = len(main[(main['外呼状态'].isin(['高意向','低意向','无需外呼'])) & (~main['最新跟进状态'].isin(['未分配','待查看','待联系']))])
    o = len(order)
    return [t,v,a,f,o]

stages = ["总客资","有效客资","分配数","跟进数","成交数"]
vals = funnel(df_main_filtered, df_order_filtered)
fig = go.Figure(go.Funnel(y=stages, x=vals, textinfo="value+percent initial"))
st.plotly_chart(fig, use_container_width=True)

# ----------------------------- 趋势 -----------------------------
st.header("📈 转化率趋势")
if not df_main_filtered.empty:
    daily = df_main_filtered.groupby(df_main_filtered['日期'].dt.date).apply(
        lambda x: pd.Series({
            '总客资':len(x),
            '有效客资':len(x[x['外呼状态'].isin(['高意向','低意向','无需外呼'])]),
            '分配数':len(x[(x['外呼状态'].isin(['高意向','低意向','无需外呼'])) & (~x['最新跟进状态'].isin(['未分配']))]),
            '跟进数':len(x[(x['外呼状态'].isin(['高意向','低意向','无需外呼'])) & (~x['最新跟进状态'].isin(['未分配','待查看','待联系']))])
        })
    ).reset_index()
    d_ord = df_order_filtered.groupby(df_order_filtered['日期'].dt.date).size().reset_index(name='成交数')
    daily = daily.merge(d_ord, on='日期', how='left').fillna(0)
    daily['有效率'] = daily['有效客资'] / daily['总客资'].replace(0,pd.NA)
    daily['分配率'] = daily['分配数'] / daily['有效客资'].replace(0,pd.NA)
    daily['跟进率'] = daily['跟进数'] / daily['分配数'].replace(0,pd.NA)
    daily['成交率'] = daily['成交数'] / daily['跟进数'].replace(0,pd.NA)

    fig = go.Figure()
    fig.add_trace(go.Scatter(x=daily['日期'], y=daily['有效率'], name='有效率'))
    fig.add_trace(go.Scatter(x=daily['日期'], y=daily['分配率'], name='分配率'))
    fig.add_trace(go.Scatter(x=daily['日期'], y=daily['跟进率'], name='跟进率'))
    fig.add_trace(go.Scatter(x=daily['日期'], y=daily['成交率'], name='成交率'))
    fig.update_layout(yaxis_tickformat=".0%")
    st.plotly_chart(fig, use_container_width=True)

# ----------------------------- 销售 -----------------------------
st.header("📊 各品牌销售额")
if not df_order_filtered.empty:
    bs = df_order_filtered.groupby('品牌')['订单金额'].sum().sort_values(ascending=False).head(10).reset_index()
    st.plotly_chart(px.bar(bs, x='品牌', y='订单金额', color='订单金额'), use_container_width=True)

st.subheader("销售额分布")
t1,t2,t3 = st.tabs(["品类","片区","运营中心"])
with t1:
    if '品类' in df_order_filtered.columns:
        c = df_order_filtered.groupby('品类')['订单金额'].sum().reset_index()
        st.plotly_chart(px.bar(c, x='品类', y='订单金额'), use_container_width=True)
with t2:
    if '片区' in df_order_filtered.columns:
        r = df_order_filtered.groupby('片区')['订单金额'].sum().reset_index()
        st.plotly_chart(px.bar(r, x='片区', y='订单金额'), use_container_width=True)
with t3:
    if '运营中心' in df_order_filtered.columns:
        ce = df_order_filtered.groupby('运营中心')['订单金额'].sum().sort_values(ascending=False).head(15).reset_index()
        st.plotly_chart(px.bar(ce, x='运营中心', y='订单金额'), use_container_width=True)

# ----------------------------- 市区订单热力图 -----------------------------
st.header("🗺️ 市区订单热力图")
if '市区' in df_order_filtered.columns:
    city_amount = df_order_filtered.groupby('市区')['订单金额'].sum().reset_index()
    city_amount = city_amount.sort_values('订单金额', ascending=False)
    
    top_n = 20
    if len(city_amount) > top_n:
        city_amount = city_amount.head(top_n)
    
    fig_city = px.bar(
        city_amount,
        x='市区',
        y='订单金额',
        color='订单金额',
        color_continuous_scale='YlOrRd',
        title=f'各市区订单金额分布（Top{top_n}）',
        labels={'订单金额': '订单金额（元）', '市区': '市区'}
    )
    fig_city.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig_city, use_container_width=True)
    
    with st.expander("查看详细数据"):
        st.dataframe(city_amount.style.format({'订单金额': '{:,.0f}'}))
else:
    st.warning("订单表中没有找到「市区」字段，无法绘制热力图。")

st.caption("数据实时更新 | 有效客资：高/低意向/无需外呼")
