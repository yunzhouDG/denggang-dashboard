import streamlit as st
import pandas as pd
import sqlite3
import zipfile
import tempfile
import os
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

# ------------------- 页面设置 -------------------
st.set_page_config(
    layout="wide",
    page_title="新零售数据看板",
    page_icon="📊"
)

# 自定义样式
st.markdown("""
<style>
[data-testid="stMetricValue"] {
    font-size: 26px !important;
    font-weight: bold !important;
    color: #2F4F4F;
}
[data-testid="stMetricLabel"] {
    font-size: 16px !important;
    color: #696969;
}
.block-container {
    padding-top: 1.5rem;
    padding-bottom: 1.5rem;
}
</style>
""", unsafe_allow_html=True)

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

    if '意向品牌' in df_main.columns:
        df_main.rename(columns={'意向品牌': '品牌'}, inplace=True)
    if '获取时间' in df_main.columns:
        df_main.rename(columns={'获取时间': '日期'}, inplace=True)
    if '运中' in df_order.columns:
        df_order.rename(columns={'运中': '运营中心'}, inplace=True)

    for col in ['日期']:
        if col in df_main.columns:
            df_main[col] = pd.to_datetime(df_main[col], errors='coerce')
        if col in df_order.columns:
            df_order[col] = pd.to_datetime(df_order[col], errors='coerce')

    if '订单金额' in df_order.columns:
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

# ----------------------------- 品牌筛选函数 -----------------------------
def filter_by_brand(df, selected_brands):
    if not selected_brands:
        return df.copy()

    brand_col = None
    if '品牌' in df.columns:
        brand_col = '品牌'
    elif '意向品牌' in df.columns:
        brand_col = '意向品牌'

    if brand_col is None or brand_col not in df.columns:
        return df.copy()

    try:
        brands = df[brand_col].fillna("未知").tolist()
    except:
        return df.copy()

    cat_col = '品类' if '品类' in df.columns else None
    try:
        cats = df[cat_col].fillna("未知").tolist() if cat_col else ['未知'] * len(df)
    except:
        cats = ['未知'] * len(df)

    keep = []
    for brand, cat in zip(brands, cats):
        flag = False
        for item in selected_brands:
            if item == '美的' and brand == '美的':
                flag = True
                break
            if item == '东芝' and brand == '东芝':
                flag = True
                break
            if item == '小天鹅' and brand == '小天鹅':
                flag = True
                break
            if item == 'COLMO' and brand == 'COLMO':
                flag = True
                break
            if item == '美的厨热' and brand == '美的' and cat == '厨热':
                flag = True
                break
            if item == '美的冰箱' and brand == '美的' and cat == '冰箱':
                flag = True
                break
            if item == '美的空调' and brand == '美的' and cat == '空调':
                flag = True
                break
            if item == '洗衣机汇总':
                if brand == '小天鹅' or (brand == '美的' and cat == '洗衣机'):
                    flag = True
                    break
        keep.append(flag)

    try:
        return df[keep].copy()
    except:
        return df.copy()

# ----------------------------- 工具函数 -----------------------------
def get_unique_sorted(series):
    return sorted(series.dropna().unique())

def fmt_wan(x):
    return f"{x/10000:.1f} 万"

# ----------------------------- 侧边栏筛选 -----------------------------
st.sidebar.header("🔍 数据筛选")

# 📅 日期范围（兼容所有版本，零报错）
st.sidebar.subheader("📅 日期范围")
if '日期' in df_main.columns and not df_main['日期'].isna().all():
    min_date = df_main['日期'].min().date()
    max_date = df_main['日期'].max().date()
else:
    min_date = datetime.today().date()
    max_date = datetime.today().date()

date_range = st.sidebar.date_input(
    "选择日期",
    [min_date, max_date],
    min_value=min_date,
    max_value=max_date
)

# 品牌
custom_brands = ['美的', '东芝', '小天鹅', 'COLMO', '美的厨热', '美的冰箱', '美的空调', '洗衣机汇总']
selected_brands = st.sidebar.multiselect("品牌", custom_brands, default=custom_brands)

# 品类
category_options = get_unique_sorted(df_main['品类']) if '品类' in df_main.columns else []
selected_categories = st.sidebar.multiselect("品类", category_options, default=category_options)

# 片区
region_options = get_unique_sorted(df_main['片区']) if '片区' in df_main.columns else []
selected_regions = st.sidebar.multiselect("片区", region_options, default=region_options)

# 运营中心
center_options = get_unique_sorted(df_main['运营中心']) if '运营中心' in df_main.columns else []
selected_centers = st.sidebar.multiselect("运营中心", center_options, default=center_options)

# ----------------------------- 数据筛选 -----------------------------
def filter_main(df, date_range, categories, regions, centers):
    if '日期' in df.columns and len(date_range) == 2:
        start, end = date_range
        df = df[(df['日期'].dt.date >= start) & (df['日期'].dt.date <= end)]
    if categories and '品类' in df.columns:
        df = df[df['品类'].isin(categories)]
    if regions and '片区' in df.columns:
        df = df[df['片区'].isin(regions)]
    if centers and '运营中心' in df.columns:
        df = df[df['运营中心'].isin(centers)]
    return df

def filter_order(df, date_range, categories, regions, centers):
    if '日期' in df.columns and len(date_range) == 2:
        start, end = date_range
        df = df[(df['日期'].dt.date >= start) & (df['日期'].dt.date <= end)]
    if categories and '品类' in df.columns:
        df = df[df['品类'].isin(categories)]
    if regions and '片区' in df.columns:
        df = df[df['片区'].isin(regions)]
    if centers and '运营中心' in df.columns:
        df = df[df['运营中心'].isin(centers)]
    return df

df_main_filtered = filter_main(df_main, date_range, selected_categories, selected_regions, selected_centers)
df_main_filtered = filter_by_brand(df_main_filtered, selected_brands)

df_order_filtered = filter_order(df_order, date_range, selected_categories, selected_regions, selected_centers)
df_order_filtered = filter_by_brand(df_order_filtered, selected_brands)

# ----------------------------- 核心指标 -----------------------------
st.title("🏬 天猫新零售数据看板")
st.markdown("<hr style='margin:10px 0;'>", unsafe_allow_html=True)

total_leads = len(df_main_filtered)
valid_leads = len(df_main_filtered[df_main_filtered['外呼状态'].isin(['高意向', '低意向', '无需外呼'])]) if '外呼状态' in df_main_filtered.columns else 0
total_orders = len(df_order_filtered)
total_amount = df_order_filtered['订单金额'].sum() if '订单金额' in df_order_filtered.columns else 0

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric("总客资数", f"{total_leads:,}")
with col2:
    st.metric("有效客资数", f"{valid_leads:,}")
with col3:
    st.metric("成交订单数", f"{total_orders:,}")
with col4:
    st.metric("成交总金额", fmt_wan(total_amount))

# ----------------------------- 环比 -----------------------------
def calc_change(curr, prev):
    return (curr - prev) / prev if prev != 0 else None

today = datetime.today().date()
yesterday = today - timedelta(days=1)

if '日期' in df_order_filtered.columns and '订单金额' in df_order_filtered.columns:
    amt_today = df_order_filtered[df_order_filtered['日期'].dt.date == today]['订单金额'].sum()
    amt_yesd = df_order_filtered[df_order_filtered['日期'].dt.date == yesterday]['订单金额'].sum()
    day_chg = calc_change(amt_today, amt_yesd)
    if day_chg is not None:
        st.sidebar.metric("日环比（金额）", f"{day_chg:.1%}")

first_day_cur = datetime(today.year, today.month, 1).date()
first_day_prev = datetime(today.year if today.month>1 else today.year-1,
                          today.month-1 if today.month>1 else 12, 1).date()

if '日期' in df_order_filtered.columns and '订单金额' in df_order_filtered.columns:
    amt_cur_m = df_order_filtered[df_order_filtered['日期'].dt.date >= first_day_cur]['订单金额'].sum()
    amt_pre_m = df_order_filtered[(df_order_filtered['日期'].dt.date >= first_day_prev) &
                                  (df_order_filtered['日期'].dt.date < first_day_cur)]['订单金额'].sum()
    mon_chg = calc_change(amt_cur_m, amt_pre_m)
    if mon_chg is not None:
        st.sidebar.metric("月环比（金额）", f"{mon_chg:.1%}")

# ----------------------------- 转化漏斗 -----------------------------
st.divider()
st.header("📉 整体转化漏斗")

def get_funnel(main_df, order_df):
    total = len(main_df)
    if '外呼状态' in main_df.columns:
        valid = len(main_df[main_df['外呼状态'].isin(['高意向', '低意向', '无需外呼'])])
        assign = len(main_df[(main_df['外呼状态'].isin(['高意向', '低意向', '无需外呼'])) &
                             (~main_df['最新跟进状态'].isin(['未分配']))]) if '最新跟进状态' in main_df.columns else 0
        follow = len(main_df[(main_df['外呼状态'].isin(['高意向', '低意向', '无需外呼'])) &
                             (~main_df['最新跟进状态'].isin(['未分配', '待查看', '待联系']))]) if '最新跟进状态' in main_df.columns else 0
    else:
        valid = assign = follow = 0
    orders = len(order_df)
    return [total, valid, assign, follow, orders]

stages = ["总客资", "有效客资", "已分配", "已跟进", "成交"]
vals = get_funnel(df_main_filtered, df_order_filtered)

fig_fun = go.Figure(go.Funnel(
    y=stages, x=vals,
    textinfo="value+percent initial",
    marker={"color": ["#4C78A8", "#72B7F2", "#F58518", "#E45756", "#54A24B"]}
))
fig_fun.update_layout(height=400, title_text="客资转化漏斗", title_font_size=16)
st.plotly_chart(fig_fun, use_container_width=True)

# ----------------------------- 转化率趋势 -----------------------------
st.divider()
st.header("📈 每日转化率趋势")

if '日期' in df_main_filtered.columns and not df_main_filtered['日期'].isna().all():
    daily = df_main_filtered.groupby(df_main_filtered['日期'].dt.date).apply(
        lambda x: pd.Series({
            '总客资': len(x),
            '有效客资': len(x[x['外呼状态'].isin(['高意向', '低意向', '无需外呼'])]) if '外呼状态' in x.columns else 0,
            '分配数': len(x[(x['外呼状态'].isin(['高意向', '低意向', '无需外呼'])) &
                            (~x['最新跟进状态'].isin(['未分配']))]) if '外呼状态' in x.columns and '最新跟进状态' in x.columns else 0,
            '跟进数': len(x[(x['外呼状态'].isin(['高意向', '低意向', '无需外呼'])) &
                            (~x['最新跟进状态'].isin(['未分配', '待查看', '待联系']))]) if '外呼状态' in x.columns and '最新跟进状态' in x.columns else 0
        })
    ).reset_index()

    daily_ord = df_order_filtered.groupby(df_order_filtered['日期'].dt.date).size().reset_index(name='成交数') if '日期' in df_order_filtered.columns else pd.DataFrame(columns=['日期','成交数'])
    daily = daily.merge(daily_ord, on='日期', how='left').fillna(0)

    daily['有效率'] = daily['有效客资'] / daily['总客资'].replace(0, pd.NA)
    daily['分配率'] = daily['分配数'] / daily['有效客资'].replace(0, pd.NA)
    daily['跟进率'] = daily['跟进数'] / daily['分配数'].replace(0, pd.NA)
    daily['成交率'] = daily['成交数'] / daily['跟进数'].replace(0, pd.NA)

    fig_line = go.Figure()
    fig_line.add_trace(go.Scatter(x=daily['日期'], y=daily['有效率'], name='有效率', line=dict(width=3)))
    fig_line.add_trace(go.Scatter(x=daily['日期'], y=daily['分配率'], name='分配率', line=dict(width=3)))
    fig_line.add_trace(go.Scatter(x=daily['日期'], y=daily['跟进率'], name='跟进率', line=dict(width=3)))
    fig_line.add_trace(go.Scatter(x=daily['日期'], y=daily['成交率'], name='成交率', line=dict(width=3, color='#E45756')))
    fig_line.update_layout(height=350, yaxis_tickformat='.1%', title="日转化率趋势")
    st.plotly_chart(fig_line, use_container_width=True)
else:
    st.info("暂无日期数据，无法展示趋势")

# ----------------------------- 销售额排行 -----------------------------
st.divider()
st.header("💰 销售额分析")

col_left, col_right = st.columns(2)

with col_left:
    st.subheader("品牌销售额排行")
    if not df_order_filtered.empty and '品牌' in df_order_filtered.columns and '订单金额' in df_order_filtered.columns:
        brand_sale = df_order_filtered.groupby('品牌')['订单金额'].sum().reset_index()
        brand_sale['订单金额_万'] = brand_sale['订单金额'] / 10000
        brand_sale = brand_sale.sort_values('订单金额', ascending=False).head(10)
        fig_b = px.bar(brand_sale, x='品牌', y='订单金额_万',
                       color='订单金额_万', color_continuous_scale='Blues',
                       labels={'订单金额_万':'销售额(万)'})
        fig_b.update_layout(height=380)
        st.plotly_chart(fig_b, use_container_width=True)
    else:
        st.info("暂无品牌销售数据")

with col_right:
    st.subheader("品类销售额排行")
    if '品类' in df_order_filtered.columns and '订单金额' in df_order_filtered.columns:
        cat_sale = df_order_filtered.groupby('品类')['订单金额'].sum().reset_index()
        cat_sale['订单金额_万'] = cat_sale['订单金额'] / 10000
        cat_sale = cat_sale.sort_values('订单金额', ascending=False)
        fig_c = px.bar(cat_sale, x='品类', y='订单金额_万',
                       color='订单金额_万', color_continuous_scale='Greens',
                       labels={'订单金额_万':'销售额(万)'})
        fig_c.update_layout(height=380)
        st.plotly_chart(fig_c, use_container_width=True)
    else:
        st.info("暂无品类销售数据")

# ----------------------------- 区域结构 -----------------------------
st.divider()
st.header("🌍 区域销售结构")

tab1, tab2 = st.tabs(["片区销售额", "运营中心销售额"])
with tab1:
    if '片区' in df_order_filtered.columns and '订单金额' in df_order_filtered.columns:
        reg = df_order_filtered.groupby('片区')['订单金额'].sum().reset_index()
        reg['万'] = reg['订单金额'] / 10000
        fig = px.bar(reg, x='片区', y='万', color='万', color_continuous_scale='Oranges')
        st.plotly_chart(fig, use_container_width=True)
with tab2:
    if '运营中心' in df_order_filtered.columns and '订单金额' in df_order_filtered.columns:
        cen = df_order_filtered.groupby('运营中心')['订单金额'].sum().reset_index().sort_values('订单金额', ascending=False).head(15)
        cen['万'] = cen['订单金额'] / 10000
        fig = px.bar(cen, x='运营中心', y='万', color='万', color_continuous_scale='purples')
        st.plotly_chart(fig, use_container_width=True)

st.caption("✅ 数据实时刷新 | 金额单位：万元")
