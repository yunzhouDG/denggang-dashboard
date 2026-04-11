import streamlit as st
import pandas as pd
import sqlite3
import zipfile
import tempfile
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta  # 新增：用于月环比
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(layout="wide", page_title="天猫新零售数据看板", page_icon="📊")

# 自定义样式（略，与原代码相同）
st.markdown("""
<style>
    .stApp { background-color: #f5f7fb; font-family: 'Segoe UI', 'Roboto', sans-serif; }
    .metric-card {
        background: linear-gradient(135deg, #ffffff 0%, #f8faff 100%);
        border-radius: 20px; padding: 1rem 1.2rem;
        box-shadow: 0 4px 12px rgba(0,0,0,0.05);
        border: 1px solid rgba(66, 153, 225, 0.1);
        transition: all 0.2s ease;
    }
    .metric-card:hover { transform: translateY(-2px); box-shadow: 0 8px 20px rgba(0,0,0,0.08); }
    .metric-label { font-size: 0.85rem; font-weight: 600; color: #4a5568; margin-bottom: 0.5rem; }
    .metric-value { font-size: 2.2rem; font-weight: 700; color: #1e293b; line-height: 1.2; }
    .metric-compare { font-size: 0.75rem; color: #64748b; margin-top: 0.5rem; display: flex; gap: 0.8rem; }
    .compare-up { color: #10b981; }
    .compare-down { color: #ef4444; }
    .dashboard-title {
        font-size: 2rem; font-weight: 700;
        background: linear-gradient(120deg, #2563eb, #7c3aed);
        -webkit-background-clip: text; background-clip: text; color: transparent;
        margin-bottom: 0.5rem;
    }
    .section-header {
        font-size: 1.4rem; font-weight: 600; color: #1f2937;
        border-left: 5px solid #3b82f6; padding-left: 1rem;
        margin: 1.5rem 0 1rem 0;
    }
    .stPlotlyChart { background: white; border-radius: 20px; padding: 0.5rem; box-shadow: 0 1px 3px rgba(0,0,0,0.03); }
</style>
""", unsafe_allow_html=True)

# 省份中心坐标（保留未使用）
PROVINCE_CENTER_STD = { ... }  # 与原代码相同，此处省略以节省篇幅

# 品牌标准化等辅助函数（与原代码相同）
def standardize_brand(brand_val):
    if pd.isna(brand_val):
        return "未知"
    s = str(brand_val).strip().lower()
    if '小天鹅' in s or 'swan' in s:
        return "小天鹅"
    if '东芝' in s or 'toshiba' in s:
        return "东芝"
    if 'colmo' in s or '科摩' in s:
        return "colmo"
    if '美的' in s or 'midea' in s:
        return "美的"
    return brand_val

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

def filter_by_date(df, date_range):
    if "日期" not in df.columns or df["日期"].isna().all():
        return df
    d_start, d_end = date_range
    return df[(df["日期"].dt.date >= d_start) & (df["日期"].dt.date <= d_end)]

def normalize_province_name(name):
    # 与原代码相同，省略
    ...

def extract_province_from_raw(province_raw):
    # 与原代码相同，省略
    ...

# ---------- 新增：统一的数据筛选与指标计算函数 ----------
def filter_and_compute_metrics(df_main, df_order, start_date, end_date, sel_brand, sel_cat, sel_center, sel_area):
    """
    根据日期范围及筛选条件，返回过滤后的客资表和订单表，并计算四个核心指标。
    返回: (df_m, df_o, total_leads, valid_leads, order_count, total_amount)
    """
    # 客资表筛选
    df_m = filter_by_date(df_main, (start_date, end_date))
    df_m = apply_brand_filter(df_m, sel_brand)
    if sel_cat:
        df_m = df_m[df_m["品类"].isin(sel_cat)]
    if sel_center:
        df_m = df_m[df_m["运营中心"].isin(sel_center)]
    if sel_area:
        df_m = df_m[df_m["片区"].isin(sel_area)]

    # 订单表筛选（片区不适用于订单表）
    df_o = filter_by_date(df_order, (start_date, end_date))
    df_o = apply_brand_filter(df_o, sel_brand)
    if sel_cat:
        df_o = df_o[df_o["品类"].isin(sel_cat)]
    if sel_center:
        df_o = df_o[df_o["运营中心"].isin(sel_center)]

    # 计算指标
    total_leads = len(df_m)
    valid_mask = df_m["外呼状态"].isin(["高意向", "低意向", "无需外呼"])
    valid_leads = valid_mask.sum()
    order_count = len(df_o)
    total_amount = df_o["订单金额"].sum() if not df_o.empty else 0.0

    return df_m, df_o, total_leads, valid_leads, order_count, total_amount

def get_previous_period_range(start_date, end_date, period='day'):
    """
    根据当前日期范围计算上一周期的起止日期。
    period: 'day' 日环比，'month' 月环比
    返回 (prev_start, prev_end)
    """
    days = (end_date - start_date).days
    if period == 'day':
        # 前推相同天数，结束于当前开始日期的前一天
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=days)
    elif period == 'month':
        # 前推一个月，结束于当前开始日期减一个月的前一天（保持相同天数）
        prev_end = start_date - relativedelta(months=1)
        prev_start = prev_end - timedelta(days=days)
    else:
        raise ValueError("period must be 'day' or 'month'")
    return prev_start, prev_end

def format_compare(current, previous):
    """格式化环比文本，返回 (html_string, 变化率数值)"""
    if previous is None or previous == 0:
        return '<span style="color:#94a3b8;">无数据</span>', None
    change = (current - previous) / previous
    arrow = "▲" if change >= 0 else "▼"
    color_class = "compare-up" if change >= 0 else "compare-down"
    percent = f"{abs(change)*100:.1f}%"
    html = f'<span class="{color_class}">{arrow} {percent}</span>'
    return html, change

# ---------- 主程序 ----------
df_main, df_order = load_data()  # load_data 与原代码相同
if df_main.empty:
    st.error("客资明细表为空，请检查数据源")
    st.stop()

# 提取标准化后的省份和城市（与原代码相同）
df_main["省份_客资"] = df_main["省份_raw"].apply(extract_province_from_raw)
df_main["城市_客资"] = df_main["城市_raw"]
df_order["省份_订单"] = df_order["省份_raw"].apply(extract_province_from_raw)
df_order["城市_订单"] = df_order["城市_raw"]

# 获取可选项（与原代码相同）
all_brands = set(df_main["品牌"].dropna().unique()) | set(df_order["品牌"].dropna().unique())
actual_brands = sorted([b for b in all_brands if b and b != "未知"])
actual_cats = sorted([c for c in df_main["品类"].dropna().unique() if c and c != "未知"])
actual_centers = sorted([c for c in df_main["运营中心"].dropna().unique() if c and c != "未知"])
actual_areas = sorted([a for a in df_main["片区"].dropna().unique() if a and a != "未知"])
brand_options = actual_brands + ["洗衣机汇总", "美的厨热", "美的冰箱", "美的空调"]

# 侧边栏筛选（与原代码相同）
st.sidebar.markdown("## 🎛️ 筛选面板")
if not df_main["日期"].isna().all():
    min_date = df_main["日期"].min().date()
    max_date = df_main["日期"].max().date()
else:
    min_date = datetime.today().date()
    max_date = datetime.today().date()

start_date = st.sidebar.date_input("开始日期", min_date)
end_date = st.sidebar.date_input("结束日期", max_date)

col1_s, col2_s = st.sidebar.columns(2)
with col1_s:
    sel_brand = st.multiselect("🏷️ 品牌", brand_options, default=[], placeholder="请选择品牌")
    sel_cat = st.multiselect("📦 品类", actual_cats, default=[], placeholder="请选择品类")
with col2_s:
    sel_area = st.multiselect("🗺️ 片区", actual_areas, default=[], placeholder="请选择片区")
    sel_center = st.multiselect("📍 运营中心", actual_centers, default=[], placeholder="请选择运营中心")

# ---------- 获取当前周期指标 ----------
df_m_curr, df_o_curr, total_leads, valid_leads, order_count, total_amount = filter_and_compute_metrics(
    df_main, df_order, start_date, end_date, sel_brand, sel_cat, sel_center, sel_area
)

# ---------- 计算日环比 ----------
day_prev_start, day_prev_end = get_previous_period_range(start_date, end_date, period='day')
_, _, total_leads_day_prev, valid_leads_day_prev, order_count_day_prev, amount_day_prev = filter_and_compute_metrics(
    df_main, df_order, day_prev_start, day_prev_end, sel_brand, sel_cat, sel_center, sel_area
)

# ---------- 计算月环比 ----------
month_prev_start, month_prev_end = get_previous_period_range(start_date, end_date, period='month')
_, _, total_leads_month_prev, valid_leads_month_prev, order_count_month_prev, amount_month_prev = filter_and_compute_metrics(
    df_main, df_order, month_prev_start, month_prev_end, sel_brand, sel_cat, sel_center, sel_area
)

# 格式化环比文本（四个指标各自生成日环比和月环比）
def get_compare_html(current, prev_day, prev_month):
    day_html, _ = format_compare(current, prev_day)
    month_html, _ = format_compare(current, prev_month)
    return f'<div class="metric-compare"><span>日环比 {day_html}</span><span>月环比 {month_html}</span></div>'

compare_leads = get_compare_html(total_leads, total_leads_day_prev, total_leads_month_prev)
compare_valid = get_compare_html(valid_leads, valid_leads_day_prev, valid_leads_month_prev)
compare_orders = get_compare_html(order_count, order_count_day_prev, order_count_month_prev)
compare_amount = get_compare_html(total_amount, amount_day_prev, amount_month_prev)

# 标题与日期显示（与原代码相同）
latest_date = max_date.strftime("%Y年%m月%d日") if not df_main["日期"].isna().all() else "未知"
st.markdown('<div class="dashboard-title">🏬 天猫新零售数据看板</div>', unsafe_allow_html=True)
st.markdown(f"<div style='color:#64748b; margin-bottom:1.2rem;'>数据更新至 {latest_date}</div>", unsafe_allow_html=True)

# 指标卡片（添加环比信息）
total_wan = total_amount / 10000
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">📋 总客资</div>
        <div class="metric-value">{total_leads:,}</div>
        {compare_leads}
    </div>
    """, unsafe_allow_html=True)
with col2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">✅ 有效客资</div>
        <div class="metric-value">{valid_leads:,}</div>
        {compare_valid}
    </div>
    """, unsafe_allow_html=True)
with col3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">🛒 成交单量</div>
        <div class="metric-value">{order_count:,}</div>
        {compare_orders}
    </div>
    """, unsafe_allow_html=True)
with col4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">💰 总金额（万元）</div>
        <div class="metric-value">{total_wan:.2f} 万</div>
        {compare_amount}
    </div>
    """, unsafe_allow_html=True)

# 后续原有的转化漏斗、趋势图、销售额分布等图表完全保持不变
# （此处省略后续代码，与原程序一致）
