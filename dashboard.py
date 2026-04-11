import streamlit as st
import pandas as pd
import sqlite3
import zipfile
import tempfile
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(layout="wide", page_title="天猫新零售数据看板", page_icon="📊")

# ==================== 自定义样式 ====================
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

# ==================== 辅助函数 ====================
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

# ==================== 数据加载 ====================
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

    # 省份/城市原始字段
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

# ==================== 环比相关函数 ====================
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

def get_compare_html(current, prev_day, prev_month):
    day_html, _ = format_compare(current, prev_day)
    month_html, _ = format_compare(current, prev_month)
    return f'<div class="metric-compare"><span>日环比 {day_html}</span><span>月环比 {month_html}</span></div>'

# ==================== 主程序 ====================
df_main, df_order = load_data()
if df_main.empty:
    st.error("客资明细表为空，请检查数据源")
    st.stop()

# 提取标准化后的省份和城市
df_main["省份_客资"] = df_main["省份_raw"].apply(extract_province_from_raw)
df_main["城市_客资"] = df_main["城市_raw"]
df_order["省份_订单"] = df_order["省份_raw"].apply(extract_province_from_raw)
df_order["城市_订单"] = df_order["城市_raw"]

# 获取可选项
all_brands = set(df_main["品牌"].dropna().unique()) | set(df_order["品牌"].dropna().unique())
actual_brands = sorted([b for b in all_brands if b and b != "未知"])
actual_cats = sorted([c for c in df_main["品类"].dropna().unique() if c and c != "未知"])
actual_centers = sorted([c for c in df_main["运营中心"].dropna().unique() if c and c != "未知"])
actual_areas = sorted([a for a in df_main["片区"].dropna().unique() if a and a != "未知"])
brand_options = actual_brands + ["洗衣机汇总", "美的厨热", "美的冰箱", "美的空调"]

# 侧边栏筛选
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

# ---------- 当前周期指标 ----------
df_m_curr, df_o_curr, total_leads, valid_leads, order_count, total_amount = filter_and_compute_metrics(
    df_main, df_order, start_date, end_date, sel_brand, sel_cat, sel_center, sel_area
)

# ---------- 日环比 ----------
day_prev_start, day_prev_end = get_previous_period_range(start_date, end_date, 'day')
_, _, total_leads_day_prev, valid_leads_day_prev, order_count_day_prev, amount_day_prev = filter_and_compute_metrics(
    df_main, df_order, day_prev_start, day_prev_end, sel_brand, sel_cat, sel_center, sel_area
)

# ---------- 月环比 ----------
month_prev_start, month_prev_end = get_previous_period_range(start_date, end_date, 'month')
_, _, total_leads_month_prev, valid_leads_month_prev, order_count_month_prev, amount_month_prev = filter_and_compute_metrics(
    df_main, df_order, month_prev_start, month_prev_end, sel_brand, sel_cat, sel_center, sel_area
)

compare_leads = get_compare_html(total_leads, total_leads_day_prev, total_leads_month_prev)
compare_valid = get_compare_html(valid_leads, valid_leads_day_prev, valid_leads_month_prev)
compare_orders = get_compare_html(order_count, order_count_day_prev, order_count_month_prev)
compare_amount = get_compare_html(total_amount, amount_day_prev, amount_month_prev)

# 页面标题
latest_date = max_date.strftime("%Y年%m月%d日") if not df_main["日期"].isna().all() else "未知"
st.markdown('<div class="dashboard-title">🏬 天猫新零售数据看板</div>', unsafe_allow_html=True)
st.markdown(f"<div style='color:#64748b; margin-bottom:1.2rem;'>数据更新至 {latest_date}</div>", unsafe_allow_html=True)

# 指标卡片
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

# ==================== 转化漏斗 ====================
st.markdown('<div class="section-header">📉 转化漏斗</div>', unsafe_allow_html=True)
if "最新跟进状态" in df_m_curr.columns and not df_m_curr.empty:
    valid_mask_curr = df_m_curr["外呼状态"].isin(["高意向", "低意向", "无需外呼"])
    assigned = df_m_curr[valid_mask_curr & (df_m_curr["最新跟进状态"] != "未分配")].shape[0]
    followed = df_m_curr[valid_mask_curr & (~df_m_curr["最新跟进状态"].isin(["未分配", "待查看", "待联系"]))].shape[0]
else:
    assigned = 0
    followed = 0

funnel_labels = ["总客资", "有效客资", "已分配", "已跟进", "成交"]
funnel_values = [total_leads, valid_leads, assigned, followed, order_count]
colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd']
fig_funnel = go.Figure(go.Funnel(
    y=funnel_labels,
    x=funnel_values,
    marker=dict(color=colors),
    textinfo="value",
    texttemplate='%{value:,.0f}',
    textposition="inside",
    connector=dict(line=dict(color="grey", width=2))
))
fig_funnel.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(family="Segoe UI", size=12))
st.plotly_chart(fig_funnel, use_container_width=True)

# ==================== 转化率趋势 ====================
st.markdown('<div class="section-header">📈 转化率趋势</div>', unsafe_allow_html=True)

def map_ratio(r):
    return r if r <= 1.0 else 1.0 + (r - 1.0) * 0.2

if not df_m_curr.empty and "日期" in df_m_curr and not df_m_curr["日期"].isna().all():
    daily = df_m_curr.groupby(df_m_curr["日期"].dt.date).agg(
        总客资=("品牌", "count"),
        有效客资=("外呼状态", lambda x: x.isin(["高意向", "低意向", "无需外呼"]).sum())
    ).reset_index()
    valid_df = df_m_curr[df_m_curr["外呼状态"].isin(["高意向", "低意向", "无需外呼"])]
    if not valid_df.empty and "最新跟进状态" in valid_df.columns:
        daily_assign = valid_df.groupby(valid_df["日期"].dt.date).agg(
            已分配=("最新跟进状态", lambda x: (x != "未分配").sum()),
            已跟进=("最新跟进状态", lambda x: (~x.isin(["未分配", "待查看", "待联系"])).sum())
        ).reset_index()
        daily = daily.merge(daily_assign, on="日期", how="left").fillna(0)
    else:
        daily["已分配"] = 0
        daily["已跟进"] = 0
    if not df_o_curr.empty:
        daily_order = df_o_curr.groupby(df_o_curr["日期"].dt.date).size().reset_index(name="成交数")
        daily = daily.merge(daily_order, on="日期", how="left").fillna(0)
    else:
        daily["成交数"] = 0
    daily["有效率"] = daily["有效客资"] / daily["总客资"].replace(0, pd.NA)
    daily["分配率"] = daily["已分配"] / daily["有效客资"].replace(0, pd.NA)
    daily["跟进率"] = daily["已跟进"] / daily["已分配"].replace(0, pd.NA)
    daily["转化率"] = daily["成交数"] / daily["有效客资"].replace(0, pd.NA)
    for col in ["有效率", "分配率", "跟进率", "转化率"]:
        daily[col + "_mapped"] = daily[col].apply(lambda x: map_ratio(x) if pd.notna(x) else None)

    raw_ticks = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.5, 2.0, 2.5, 3.0, 3.6]
    mapped_ticks = [map_ratio(v) for v in raw_ticks]
    tick_labels = [f"{int(v*100)}%" for v in raw_ticks]
    daily["日期_中文"] = daily["日期"].apply(lambda d: d.strftime("%m月%d日"))

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(x=daily["日期"], y=daily["有效率_mapped"], mode='lines+markers', name='有效率', line=dict(color='#3b82f6', width=2)))
    fig_trend.add_trace(go.Scatter(x=daily["日期"], y=daily["分配率_mapped"], mode='lines+markers', name='分配率', line=dict(color='#10b981', width=2)))
    fig_trend.add_trace(go.Scatter(x=daily["日期"], y=daily["跟进率_mapped"], mode='lines+markers', name='跟进率', line=dict(color='#f59e0b', width=2)))
    fig_trend.add_trace(go.Scatter(x=daily["日期"], y=daily["转化率_mapped"], mode='lines+markers', name='转化率', line=dict(color='#ef4444', width=2)))
    y_max_mapped = map_ratio(3.6)
    fig_trend.update_layout(
        title="转化率趋势（有效率、分配率、跟进率、转化率）<br><sub>注：100%以上区域已压缩</sub>",
        xaxis=dict(title="日期", tickmode='array', tickvals=daily["日期"], ticktext=daily["日期_中文"], tickangle=45),
        yaxis=dict(title="比率", tickformat='.0%', range=[0, y_max_mapped], tickvals=mapped_ticks, ticktext=tick_labels),
        legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)'),
        hovermode='x unified',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)'
    )
    st.plotly_chart(fig_trend, use_container_width=True)
else:
    st.info("无有效日期数据，无法绘制趋势图")

# ==================== 销售额分布 ====================
st.markdown('<div class="section-header">💰 销售额分布</div>', unsafe_allow_html=True)
if df_o_curr.empty:
    st.warning("当前筛选条件下无订单数据，无法展示销售额分布")
else:
    tab1, tab2, tab3 = st.tabs(["🏷️ 品牌", "📦 品类", "📍 运营中心"])
    with tab1:
        brand_sale = df_o_curr.groupby("品牌")["订单金额"].sum().sort_values(ascending=False).head(10).reset_index()
        brand_sale["万元"] = brand_sale["订单金额"] / 10000
        fig1 = px.bar(brand_sale, x="品牌", y="万元", color="万元", color_continuous_scale="Blues",
                      title="品牌销售额 Top10（万元）", text="万元")
        fig1.update_traces(texttemplate='%{text:.1f}', textposition='outside')
        fig1.update_layout(plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig1, use_container_width=True)
    with tab2:
        cat_sale = df_o_curr.groupby("品类")["订单金额"].sum().reset_index()
        cat_sale["万元"] = cat_sale["订单金额"] / 10000
        fig2 = px.pie(cat_sale, names="品类", values="万元", title="品类销售额占比",
                      color_discrete_sequence=px.colors.qualitative.Pastel)
        fig2.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig2, use_container_width=True)
    with tab3:
        center_sale = df_o_curr.groupby("运营中心")["订单金额"].sum().reset_index()
        center_sale["万元"] = center_sale["订单金额"] / 10000
        fig3 = px.bar(center_sale, x="运营中心", y="万元", color="万元", color_continuous_scale="Tealgrn",
                      title="运营中心销售额（万元）")
        fig3.update_layout(xaxis_tickangle=-45, plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig3, use_container_width=True)

# ==================== 订单金额热力省 & 热力城市 ====================
st.markdown('<div class="section-header">🗺️ 订单金额热力省 & 热力城市</div>', unsafe_allow_html=True)

if df_o_curr.empty:
    st.info("暂无订单数据，无法绘制省份/城市销售额分布")
else:
    province_sale = df_o_curr.groupby("省份_订单")["订单金额"].sum().reset_index()
    province_sale = province_sale[province_sale["省份_订单"].notna() & (province_sale["省份_订单"] != "")]
    province_sale["万元"] = province_sale["订单金额"] / 10000
    province_sale_sorted = province_sale.sort_values("万元", ascending=False)

    city_sale = df_o_curr[df_o_curr["城市_订单"] != ""].groupby("城市_订单")["订单金额"].sum().reset_index()
    city_sale["万元"] = city_sale["订单金额"] / 10000
    city_sale_sorted = city_sale.sort_values("万元", ascending=False).head(20)

    col_prov, col_city = st.columns(2)
    with col_prov:
        st.subheader("🏆 省份销售额排行（热力柱状图）")
        if not province_sale_sorted.empty:
            fig_prov = px.bar(
                province_sale_sorted,
                x="万元",
                y="省份_订单",
                orientation='h',
                color="万元",
                color_continuous_scale="Blues",
                text="万元",
                title="销售额（万元）",
                labels={"万元": "销售额(万元)", "省份_订单": ""}
            )
            fig_prov.update_traces(texttemplate='%{text:.1f}', textposition='outside')
            fig_prov.update_layout(height=500, yaxis={'categoryorder':'total ascending'}, plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_prov, use_container_width=True)
        else:
            st.info("无省份销售额数据")
    with col_city:
        st.subheader("🏙️ 城市销售额排行 Top20（热力柱状图）")
        if not city_sale_sorted.empty:
            fig_city = px.bar(
                city_sale_sorted,
                x="万元",
                y="城市_订单",
                orientation='h',
                color="万元",
                color_continuous_scale="Oranges",
                text="万元",
                title="销售额（万元）",
                labels={"万元": "销售额(万元)", "城市_订单": ""}
            )
            fig_city.update_traces(texttemplate='%{text:.1f}', textposition='outside')
            fig_city.update_layout(height=500, yaxis={'categoryorder':'total ascending'}, plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_city, use_container_width=True)
        else:
            st.info("无城市销售额数据")

# ==================== 客资数量热力省 & 热力城市 ====================
st.markdown('<div class="section-header">📊 客资数量热力省 & 热力城市</div>', unsafe_allow_html=True)

if df_m_curr.empty:
    st.info("当前筛选条件下无客资数据")
else:
    province_leads = df_m_curr.groupby("省份_客资").size().reset_index(name="客资数量")
    province_leads = province_leads[province_leads["省份_客资"].notna() & (province_leads["省份_客资"] != "")]
    province_leads_sorted = province_leads.sort_values("客资数量", ascending=False)

    city_leads = df_m_curr[df_m_curr["城市_客资"] != ""].groupby("城市_客资").size().reset_index(name="客资数量")
    city_leads_sorted = city_leads.sort_values("客资数量", ascending=False).head(20)

    col_leads_prov, col_leads_city = st.columns(2)
    with col_leads_prov:
        st.subheader("🏆 省份客资排行（热力柱状图）")
        if not province_leads_sorted.empty:
            fig_leads_prov = px.bar(
                province_leads_sorted,
                x="客资数量",
                y="省份_客资",
                orientation='h',
                color="客资数量",
                color_continuous_scale="Greens",
                text="客资数量",
                title="客资数量",
                labels={"客资数量": "客资数", "省份_客资": ""}
            )
            fig_leads_prov.update_traces(texttemplate='%{text:,}', textposition='outside')
            fig_leads_prov.update_layout(height=500, yaxis={'categoryorder':'total ascending'}, plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_leads_prov, use_container_width=True)
        else:
            st.info("无省份客资数据")
    with col_leads_city:
        st.subheader("🏙️ 城市客资排行 Top20（热力柱状图）")
        if not city_leads_sorted.empty:
            fig_leads_city = px.bar(
                city_leads_sorted,
                x="客资数量",
                y="城市_客资",
                orientation='h',
                color="客资数量",
                color_continuous_scale="Tealgrn",
                text="客资数量",
                title="客资数量",
                labels={"客资数量": "客资数", "城市_客资": ""}
            )
            fig_leads_city.update_traces(texttemplate='%{text:,}', textposition='outside')
            fig_leads_city.update_layout(height=500, yaxis={'categoryorder':'total ascending'}, plot_bgcolor='rgba(0,0,0,0)')
            st.plotly_chart(fig_leads_city, use_container_width=True)
        else:
            st.info("无城市客资数据")
