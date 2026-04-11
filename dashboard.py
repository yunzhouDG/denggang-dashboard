import streamlit as st
import pandas as pd
import sqlite3
import zipfile
import tempfile
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta   # 关键导入
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(layout="wide", page_title="天猫新零售数据看板", page_icon="📊")

# ---------- 样式（与原相同，省略以节省篇幅，请自行复制之前的样式） ----------
st.markdown("""<style> ... </style>""", unsafe_allow_html=True)

# ---------- 辅助函数 ----------
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
    if not name:
        return None
    name = str(name).strip()
    if name in ['北京', '北京市']: return '北京市'
    if name in ['上海', '上海市']: return '上海市'
    if name in ['天津', '天津市']: return '天津市'
    if name in ['重庆', '重庆市']: return '重庆市'
    if name in ['广西', '广西壮族自治区']: return '广西壮族自治区'
    if name in ['内蒙古', '内蒙古自治区']: return '内蒙古自治区'
    if name in ['宁夏', '宁夏回族自治区']: return '宁夏回族自治区'
    if name in ['新疆', '新疆维吾尔自治区']: return '新疆维吾尔自治区'
    if name in ['西藏', '西藏自治区']: return '西藏自治区'
    if name.endswith('省'):
        return name
    common = ['江苏','浙江','广东','山东','河南','四川','湖北','湖南','河北','福建','安徽','辽宁','江西','陕西','山西','云南','贵州','甘肃','青海','吉林','黑龙江','海南','台湾']
    if name in common:
        return name + '省'
    return name

def extract_province_from_raw(province_raw):
    if pd.isna(province_raw) or not province_raw:
        return None
    s = str(province_raw).strip()
    if '-' in s:
        parts = s.split('-')
        if len(parts) == 2:
            province_part = parts[0].strip()
        elif len(parts) >= 3:
            province_part = parts[1].strip()
        else:
            province_part = s
    else:
        province_part = s
    return normalize_province_name(province_part)

# ---------- 加载数据（增强容错） ----------
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
        st.error(f"数据库读取失败: {e}\n请确保表名分别为 '客资明细表' 和 '订单表'")
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

    # 品牌、品类等标准化
    for df in [df_main, df_order]:
        raw_brand = df.get("品牌", df.get("意向品牌", "未知")).fillna("未知")
        df["品牌"] = raw_brand.apply(standardize_brand)
        df["品类"] = df.get("品类", "未知").fillna("未知")
        df["运营中心"] = df.get("运营中心", df.get("运中", "未知")).fillna("未知")
        df["片区"] = df.get("片区", "未知").fillna("未知")
    
    # 确保必要列存在
    if "外呼状态" not in df_main.columns:
        df_main["外呼状态"] = ""
    if "最新跟进状态" not in df_main.columns:
        df_main["最新跟进状态"] = ""
    
    # 省份/城市
    if "省份" in df_main.columns:
        df_main["省份_raw"] = df_main["省份"].fillna("").astype(str).str.strip()
    elif "省市" in df_main.columns:
        df_main["省份_raw"] = df_main["省市"].fillna("").astype(str).str.strip()
    else:
        df_main["省份_raw"] = ""
    
    if "城市" in df_main.columns:
        df_main["城市_raw"] = df_main["城市"].fillna("").astype(str).str.strip()
    elif "市区" in df_main.columns:
        df_main["城市_raw"] = df_main["市区"].fillna("").astype(str).str.strip()
    else:
        df_main["城市_raw"] = ""
    
    if "省份" in df_order.columns:
        df_order["省份_raw"] = df_order["省份"].fillna("").astype(str).str.strip()
    elif "省市" in df_order.columns:
        df_order["省份_raw"] = df_order["省市"].fillna("").astype(str).str.strip()
    else:
        df_order["省份_raw"] = ""
    
    if "城市" in df_order.columns:
        df_order["城市_raw"] = df_order["城市"].fillna("").astype(str).str.strip()
    elif "市区" in df_order.columns:
        df_order["城市_raw"] = df_order["市区"].fillna("").astype(str).str.strip()
    else:
        df_order["城市_raw"] = ""

    return df_main, df_order

# ---------- 环比相关函数 ----------
def filter_and_compute_metrics(df_main, df_order, start_date, end_date, sel_brand, sel_cat, sel_center, sel_area):
    df_m = filter_by_date(df_main, (start_date, end_date))
    df_m = apply_brand_filter(df_m, sel_brand)
    if sel_cat:
        df_m = df_m[df_m["品类"].isin(sel_cat)]
    if sel_center:
        df_m = df_m[df_m["运营中心"].isin(sel_center)]
    if sel_area:
        df_m = df_m[df_m["片区"].isin(sel_area)]

    df_o = filter_by_date(df_order, (start_date, end_date))
    df_o = apply_brand_filter(df_o, sel_brand)
    if sel_cat:
        df_o = df_o[df_o["品类"].isin(sel_cat)]
    if sel_center:
        df_o = df_o[df_o["运营中心"].isin(sel_center)]

    total_leads = len(df_m)
    valid_mask = df_m["外呼状态"].isin(["高意向", "低意向", "无需外呼"])
    valid_leads = valid_mask.sum()
    order_count = len(df_o)
    total_amount = df_o["订单金额"].sum() if not df_o.empty else 0.0
    return df_m, df_o, total_leads, valid_leads, order_count, total_amount

def get_previous_period_range(start_date, end_date, period='day'):
    days = (end_date - start_date).days
    if period == 'day':
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - timedelta(days=days)
    elif period == 'month':
        prev_end = start_date - relativedelta(months=1)
        prev_start = prev_end - timedelta(days=days)
    else:
        raise ValueError("period must be 'day' or 'month'")
    return prev_start, prev_end

def format_compare(current, previous):
    if previous is None or previous == 0:
        return '<span style="color:#94a3b8;">无数据</span>', None
    change = (current - previous) / previous
    arrow = "▲" if change >= 0 else "▼"
    color_class = "compare-up" if change >= 0 else "compare-down"
    percent = f"{abs(change)*100:.1f}%"
    html = f'<span class="{color_class}">{arrow} {percent}</span>'
    return html, change

# ---------- 主程序 ----------
df_main, df_order = load_data()
if df_main.empty:
    st.error("客资明细表为空，请检查数据源")
    st.stop()

# 提取省份/城市
df_main["省份_客资"] = df_main["省份_raw"].apply(extract_province_from_raw)
df_main["城市_客资"] = df_main["城市_raw"]
df_order["省份_订单"] = df_order["省份_raw"].apply(extract_province_from_raw)
df_order["城市_订单"] = df_order["城市_raw"]

# 可选项
all_brands = set(df_main["品牌"].dropna().unique()) | set(df_order["品牌"].dropna().unique())
actual_brands = sorted([b for b in all_brands if b and b != "未知"])
actual_cats = sorted([c for c in df_main["品类"].dropna().unique() if c and c != "未知"])
actual_centers = sorted([c for c in df_main["运营中心"].dropna().unique() if c and c != "未知"])
actual_areas = sorted([a for a in df_main["片区"].dropna().unique() if a and a != "未知"])
brand_options = actual_brands + ["洗衣机汇总", "美的厨热", "美的冰箱", "美的空调"]

# 侧边栏
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
    sel_brand = st.multiselect("🏷️ 品牌", brand_options, default=[])
    sel_cat = st.multiselect("📦 品类", actual_cats, default=[])
with col2_s:
    sel_area = st.multiselect("🗺️ 片区", actual_areas, default=[])
    sel_center = st.multiselect("📍 运营中心", actual_centers, default=[])

# 当前周期
_, _, total_leads, valid_leads, order_count, total_amount = filter_and_compute_metrics(
    df_main, df_order, start_date, end_date, sel_brand, sel_cat, sel_center, sel_area
)

# 日环比
day_prev_start, day_prev_end = get_previous_period_range(start_date, end_date, 'day')
_, _, total_leads_day_prev, valid_leads_day_prev, order_count_day_prev, amount_day_prev = filter_and_compute_metrics(
    df_main, df_order, day_prev_start, day_prev_end, sel_brand, sel_cat, sel_center, sel_area
)

# 月环比
month_prev_start, month_prev_end = get_previous_period_range(start_date, end_date, 'month')
_, _, total_leads_month_prev, valid_leads_month_prev, order_count_month_prev, amount_month_prev = filter_and_compute_metrics(
    df_main, df_order, month_prev_start, month_prev_end, sel_brand, sel_cat, sel_center, sel_area
)

def get_compare_html(current, prev_day, prev_month):
    day_html, _ = format_compare(current, prev_day)
    month_html, _ = format_compare(current, prev_month)
    return f'<div class="metric-compare"><span>日环比 {day_html}</span><span>月环比 {month_html}</span></div>'

compare_leads = get_compare_html(total_leads, total_leads_day_prev, total_leads_month_prev)
compare_valid = get_compare_html(valid_leads, valid_leads_day_prev, valid_leads_month_prev)
compare_orders = get_compare_html(order_count, order_count_day_prev, order_count_month_prev)
compare_amount = get_compare_html(total_amount, amount_day_prev, amount_month_prev)

# 页面展示
latest_date = max_date.strftime("%Y年%m月%d日") if not df_main["日期"].isna().all() else "未知"
st.markdown('<div class="dashboard-title">🏬 天猫新零售数据看板</div>', unsafe_allow_html=True)
st.markdown(f"<div style='color:#64748b; margin-bottom:1.2rem;'>数据更新至 {latest_date}</div>", unsafe_allow_html=True)

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

# 后续原有的转化漏斗、趋势图、销售额分布等图表请复制您原来的代码（此处省略）
st.info("请将您原有的图表代码（转化漏斗、趋势图、销售额分布等）粘贴在此处，以恢复完整看板。")
