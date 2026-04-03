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

def check_brand(row, selected_brands):
    brand = row.get('品牌', '')
    category = row.get('品类', '')
    for item in selected_brands:
        if item == '美的' and brand == '美的':
            return True
        if item == '东芝' and brand == '东芝':
            return True
        if item == '小天鹅' and brand == '小天鹅':
            return True
        if item == 'COLMO' and brand == 'COLMO':
            return True
        if item == '美的厨热' and brand == '美的' and category == '厨热':
            return True
        if item == '美的冰箱' and brand == '美的' and category == '冰箱':
            return True
        if item == '美的空调' and brand == '美的' and category == '空调':
            return True
        if item == '洗衣机汇总':
            if brand == '小天鹅' or (brand == '美的' and category == '洗衣机'):
                return True
    return False

def filter_by_brand(df, selected_brands):
    if not selected_brands:
        return df
    mask = df.apply(lambda row: check_brand(row, selected_brands), axis=1)
    return df[mask].copy()

def get_unique_sorted(series):
    return sorted(series.dropna().unique())

st.sidebar.header("🔍 数据筛选")

min_date = df_main['日期'].min().date() if not df_main['日期'].isna().all() else datetime.today().date()
max_date = df_main['日期'].max().date() if not df_main['日期'].isna().all() else datetime.today().date()
date_range = st.sidebar.date_input("日期范围", [min_date, max_date], min_value=min_date, max_value=max_date)

brand_options = ['美的', '东芝', '小天鹅', 'COLMO', '美的厨热', '美的冰箱', '美的空调', '洗衣机汇总']
selected_brands = st.sidebar.multiselect("品牌", brand_options, default=brand_options)

category_options = get_unique_sorted(df_main['品类'])
selected_categories = st.sidebar.multiselect("品类", category_options, default=category_options)

region_options = get_unique_sorted(df_main['片区'])
selected_regions = st.sidebar.multiselect("片区", region_options, default=region_options)

center_options = get_unique_sorted(df_main['运营中心'])
selected_centers = st.sidebar.multiselect("运营中心", center_options, default=center_options)

def filter_main(df, date_range, categories, regions, centers):
    if len(date_range) == 2:
        start, end = date_range
        df = df[(df['日期'].dt.date >= start) & (df['日期'].dt.date <= end)]
    if categories:
        df = df[df['品类'].isin(categories)]
    if regions:
        df = df[df['片区'].isin(regions)]
    if centers:
        df = df[df['运营中心'].isin(centers)]
    return df

def filter_order(df, date_range, categories, centers):
    if len(date_range) == 2:
        start, end = date_range
        df = df[(df['日期'].dt.date >= start) & (df['日期'].dt.date <= end)]
    if categories and '品类' in df.columns:
        df = df[df['品类'].isin(categories)]
    if centers and '运营中心' in df.columns:
        df = df[df['运营中心'].isin(centers)]
    return df

df_main_filtered = filter_main(df_main, date_range, selected_categories, selected_regions, selected_centers)
df_main_filtered = filter_by_brand(df_main_filtered, selected_brands)

df_order_filtered = filter_order(df_order, date_range, selected_categories, selected_centers)
df_order_filtered = filter_by_brand(df_order_filtered, selected_brands)

st.title("🏬 天猫新零售数据看板")
col1, col2, col3, col4 = st.columns(4)

total_leads = len(df_main_filtered)
valid_leads = len(df_main_filtered[df_main_filtered['外呼状态'].isin(['高意向', '低意向', '无需外呼'])])
total_orders = len(df_order_filtered)
total_amount = df_order_filtered['订单金额'].sum()

with col1:
    st.metric("总客资数", f"{total_leads:,}")
with col2:
    st.metric("有效客资数", f"{valid_leads:,}")
with col3:
    st.metric("成交数", f"{total_orders:,}")
with col4:
    st.metric("总金额", f"{total_amount:,.0f} 元")

def calc_change(current, previous):
    return (current - previous) / previous if previous != 0 else None

today = datetime.today().date()
yesterday = today - timedelta(days=1)
amount_today = df_order_filtered[df_order_filtered['日期'].dt.date == today]['订单金额'].sum()
amount_yesterday = df_order_filtered[df_order_filtered['日期'].dt.date == yesterday]['订单金额'].sum()
day_change = calc_change(amount_today, amount_yesterday)
if day_change is not None:
    st.sidebar.metric("日环比 (总金额)", f"{day_change:.1%}")
else:
    st.sidebar.info("无昨日数据")

first_day_current = datetime(today.year, today.month, 1).date()
first_day_prev = datetime(today.year - (1 if today.month == 1 else today.year), 
                          (today.month - 1) if today.month > 1 else 12, 1).date()
amount_current_month = df_order_filtered[df_order_filtered['日期'].dt.date >= first_day_current]['订单金额'].sum()
amount_prev_month = df_order_filtered[(df_order_filtered['日期'].dt.date >= first_day_prev) & 
                                      (df_order_filtered['日期'].dt.date < first_day_current)]['订单金额'].sum()
month_change = calc_change(amount_current_month, amount_prev_month)
if month_change is not None:
    st.sidebar.metric("月环比 (总金额)", f"{month_change:.1%}")
else:
    st.sidebar.info("无上月数据")

st.header("📉 转化漏斗")
def get_funnel_metrics(main_df, order_df):
    total = len(main_df)
    valid = len(main_df[main_df['外呼状态'].isin(['高意向', '低意向', '无需外呼'])])
    assigned = len(main_df[(main_df['外呼状态'].isin(['高意向', '低意向', '无需外呼'])) &
                           (~main_df['最新跟进状态'].isin(['未分配']))])
    followed = len(main_df[(main_df['外呼状态'].isin(['高意向', '低意向', '无需外呼'])) &
                           (~main_df['最新跟进状态'].isin(['未分配', '待查看', '待联系']))])
    orders = len(order_df)
    return [total, valid, assigned, followed, orders]

stages = ["总客资数", "有效客资数", "分配数", "跟进数", "成交数"]
funnel_values = get_funnel_metrics(df_main_filtered, df_order_filtered)
fig_funnel = go.Figure(go.Funnel(
    y=stages,
    x=funnel_values,
    textinfo="value+percent initial",
    marker={"color": px.colors.sequential.Blues_r}
))
fig_funnel.update_layout(title="转化漏斗")
st.plotly_chart(fig_funnel, use_container_width=True)

st.header("📈 转化率趋势")
if not df_main_filtered.empty and not df_main_filtered['日期'].isna().all():
    daily = df_main_filtered.groupby(df_main_filtered['日期'].dt.date).apply(
        lambda x: pd.Series({
            '总客资': len(x),
            '有效客资': len(x[x['外呼状态'].isin(['高意向', '低意向', '无需外呼'])]),
            '分配数': len(x[(x['外呼状态'].isin(['高意向', '低意向', '无需外呼'])) & (~x['最新跟进状态'].isin(['未分配']))]),
            '跟进数': len(x[(x['外呼状态'].isin(['高意向', '低意向', '无需外呼'])) & (~x['最新跟进状态'].isin(['未分配', '待查看', '待联系']))])
        })
    ).reset_index()
    daily_orders = df_order_filtered.groupby(df_order_filtered['日期'].dt.date).size().reset_index(name='成交数')
    daily = daily.merge(daily_orders, on='日期', how='left').fillna(0)
    daily['有效率'] = daily['有效客资'] / daily['总客资'].replace(0, pd.NA)
    daily['分配率'] = daily['分配数'] / daily['有效客资'].replace(0, pd.NA)
    daily['跟进率'] = daily['跟进数'] / daily['分配数'].replace(0, pd.NA)
    daily['成交率'] = daily['成交数'] / daily['跟进数'].replace(0, pd.NA)

    fig_line = go.Figure()
    fig_line.add_trace(go.Scatter(x=daily['日期'], y=daily['有效率'], name='有效率', mode='lines+markers'))
    fig_line.add_trace(go.Scatter(x=daily['日期'], y=daily['分配率'], name='分配率', mode='lines+markers'))
    fig_line.add_trace(go.Scatter(x=daily['日期'], y=daily['跟进率'], name='跟进率', mode='lines+markers'))
    fig_line.add_trace(go.Scatter(x=daily['日期'], y=daily['成交率'], name='成交率', mode='lines+markers'))
    fig_line.update_layout(yaxis_tickformat=".0%", xaxis_title="日期", yaxis_title="比率", legend_title="指标")
    st.plotly_chart(fig_line, use_container_width=True)
else:
    st.warning("无有效日期数据，无法绘制趋势图")

st.header("📊 各品牌销售额")
if not df_order_filtered.empty and '品牌' in df_order_filtered.columns:
    brand_sales = df_order_filtered.groupby('品牌')['订单金额'].sum().reset_index().sort_values('订单金额', ascending=False).head(10)
    fig_bar = px.bar(brand_sales, x='品牌', y='订单金额', title="Top 10 品牌销售额",
                     color='订单金额', color_continuous_scale='Viridis', text_auto='.2s')
    st.plotly_chart(fig_bar, use_container_width=True)
else:
    st.info("订单表中无品牌数据或数据为空")

st.subheader("销售额分布")
tab1, tab2, tab3 = st.tabs(["按品类", "按片区", "按运营中心"])
with tab1:
    if '品类' in df_order_filtered.columns:
        cat_sales = df_order_filtered.groupby('品类')['订单金额'].sum().reset_index()
        fig = px.bar(cat_sales, x='品类', y='订单金额', color='订单金额', color_continuous_scale='Blues')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("无品类数据")
with tab2:
    if '片区' in df_order_filtered.columns:
        reg_sales = df_order_filtered.groupby('片区')['订单金额'].sum().reset_index()
        fig = px.bar(reg_sales, x='片区', y='订单金额', color='订单金额', color_continuous_scale='Greens')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("无片区数据")
with tab3:
    if '运营中心' in df_order_filtered.columns:
        cen_sales = df_order_filtered.groupby('运营中心')['订单金额'].sum().reset_index().sort_values('订单金额', ascending=False).head(15)
        fig = px.bar(cen_sales, x='运营中心', y='订单金额', color='订单金额', color_continuous_scale='Oranges')
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("无运营中心数据")

st.caption("数据根据左侧筛选器动态更新 | 有效客资定义：外呼状态为高意向/低意向/无需外呼；分配：有效且最新跟进状态≠未分配；跟进：有效且最新跟进状态∉{未分配,待查看,待联系}")
