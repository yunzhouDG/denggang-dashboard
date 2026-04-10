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

# 自定义样式
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

# 省份中心坐标（已废弃但保留以免报错）
PROVINCE_CENTER_STD = {
    '北京市': [116.4074, 39.9042], '上海市': [121.4737, 31.2304],
    '天津市': [117.1902, 39.1256], '重庆市': [106.5044, 29.5582],
    '河北省': [114.4995, 38.1006], '山西省': [112.5624, 37.8735],
    '内蒙古自治区': [111.7510, 40.8415], '辽宁省': [123.4315, 41.8057],
    '吉林省': [125.3235, 43.8171], '黑龙江省': [126.5364, 45.8022],
    '江苏省': [118.7674, 32.0415], '浙江省': [120.1551, 30.2741],
    '安徽省': [117.2272, 31.8206], '福建省': [119.2965, 26.0745],
    '江西省': [115.8582, 28.6820], '山东省': [117.0009, 36.6758],
    '河南省': [113.6254, 34.7466], '湖北省': [114.3055, 30.5931],
    '湖南省': [112.9388, 28.2282], '广东省': [113.2644, 23.1291],
    '广西壮族自治区': [108.3661, 22.8176], '海南省': [110.1999, 20.0440],
    '四川省': [104.0668, 30.5728], '贵州省': [106.6302, 26.6477],
    '云南省': [102.8329, 24.8801], '西藏自治区': [91.1409, 29.6565],
    '陕西省': [108.9402, 34.3416], '甘肃省': [103.8343, 36.0611],
    '青海省': [101.7782, 36.6232], '宁夏回族自治区': [106.2309, 38.4872],
    '新疆维吾尔自治区': [87.6168, 43.8256], '台湾省': [121.5200, 25.0300],
    '香港特别行政区': [114.1700, 22.2700], '澳门特别行政区': [113.5400, 22.1900]
}

# 品牌标准化
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

# 加载数据
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
        raw_brand = df.get("品牌", df.get("意向品牌", "未知")).fillna("未知")
        df["品牌"] = raw_brand.apply(standardize_brand)
        df["品类"] = df.get("品类", "未知").fillna("未知")
        df["运营中心"] = df.get("运营中心", df.get("运中", "未知")).fillna("未知")
        df["片区"] = df.get("片区", "未知").fillna("未知")
    
    if "外呼状态" not in df_main.columns:
        df_main["外呼状态"] = ""
    if "最新跟进状态" not in df_main.columns:
        df_main["最新跟进状态"] = ""
    
    # ========== 客资表：省份和城市字段（优先使用“省份”“城市”）==========
    # 省份：优先取“省份”，其次“省市”
    if "省份" in df_main.columns:
        df_main["省份_raw"] = df_main["省份"].fillna("").astype(str).str.strip()
    elif "省市" in df_main.columns:
        df_main["省份_raw"] = df_main["省市"].fillna("").astype(str).str.strip()
    else:
        df_main["省份_raw"] = ""
    
    # 城市：优先取“城市”，其次“市区”
    if "城市" in df_main.columns:
        df_main["城市_raw"] = df_main["城市"].fillna("").astype(str).str.strip()
    elif "市区" in df_main.columns:
        df_main["城市_raw"] = df_main["市区"].fillna("").astype(str).str.strip()
    else:
        df_main["城市_raw"] = ""
    
    # ========== 订单表：省份和城市字段（兼容“省份”“省市”“城市”“市区”）==========
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

# 省份标准化函数
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
    """从原始省份字符串中提取标准省份名"""
    if pd.isna(province_raw) or not province_raw:
        return None
    # 如果已经是完整名称，直接标准化
    return normalize_province_name(province_raw)

# 主程序
df_main, df_order = load_data()
if df_main.empty:
    st.error("客资明细表为空，请检查数据源")
    st.stop()

# 提取客资表的标准化省份
df_main["省份_客资"] = df_main["省份_raw"].apply(extract_province_from_raw)
# 如果省份为空但城市有值，尝试通过城市映射补全（简单映射，可扩展）
def fill_province_by_city(row):
    if pd.notna(row["省份_客资"]) and row["省份_客资"]:
        return row["省份_客资"]
    city = row["城市_raw"]
    if not city:
        return None
    city_province = {
        "北京": "北京市", "上海": "上海市", "天津": "天津市", "重庆": "重庆市",
        "广州": "广东省", "深圳": "广东省", "杭州": "浙江省", "宁波": "浙江省",
        "南京": "江苏省", "苏州": "江苏省", "武汉": "湖北省", "成都": "四川省",
        "西安": "陕西省", "郑州": "河南省", "长沙": "湖南省", "合肥": "安徽省",
        "福州": "福建省", "厦门": "福建省", "青岛": "山东省", "济南": "山东省",
        "沈阳": "辽宁省", "长春": "吉林省", "哈尔滨": "黑龙江省", "昆明": "云南省",
        "贵阳": "贵州省", "南宁": "广西壮族自治区", "海口": "海南省", "兰州": "甘肃省",
        "西宁": "青海省", "银川": "宁夏回族自治区", "乌鲁木齐": "新疆维吾尔自治区",
        "呼和浩特": "内蒙古自治区", "拉萨": "西藏自治区"
    }
    for c, p in city_province.items():
        if city.startswith(c):
            return p
    return None
df_main["省份_客资"] = df_main.apply(fill_province_by_city, axis=1)

# 客资城市直接使用城市_raw
df_main["城市_客资"] = df_main["城市_raw"]

# 订单表的标准化省份和城市（用于订单金额热力图）
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
date_range = (start_date, end_date)

col1_s, col2_s = st.sidebar.columns(2)
with col1_s:
    sel_brand = st.multiselect("🏷️ 品牌", brand_options, default=[], placeholder="请选择品牌")
    sel_cat = st.multiselect("📦 品类", actual_cats, default=[], placeholder="请选择品类")
with col2_s:
    sel_area = st.multiselect("🗺️ 片区", actual_areas, default=[], placeholder="请选择片区")
    sel_center = st.multiselect("📍 运营中心", actual_centers, default=[], placeholder="请选择运营中心")

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

# 标题
latest_date = max_date.strftime("%Y年%m月%d日") if not df_main["日期"].isna().all() else "未知"
st.markdown('<div class="dashboard-title">🏬 天猫新零售数据看板</div>', unsafe_allow_html=True)
st.markdown(f"<div style='color:#64748b; margin-bottom:1.2rem;'>数据更新至 {latest_date}</div>", unsafe_allow_html=True)

# 指标卡片
total_leads = len(df_m)
valid_mask = df_m["外呼状态"].isin(["高意向", "低意向", "无需外呼"])
valid_leads = valid_mask.sum()
order_count = len(df_o)
total_amount = df_o["订单金额"].sum() if not df_o.empty else 0.0
total_wan = total_amount / 10000

col1, col2, col3, col4 = st.columns(4)
with col1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">📋 总客资</div>
        <div class="metric-value">{total_leads:,}</div>
    </div>
    """, unsafe_allow_html=True)
with col2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">✅ 有效客资</div>
        <div class="metric-value">{valid_leads:,}</div>
    </div>
    """, unsafe_allow_html=True)
with col3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">🛒 成交单量</div>
        <div class="metric-value">{order_count:,}</div>
    </div>
    """, unsafe_allow_html=True)
with col4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-label">💰 总金额（万元）</div>
        <div class="metric-value">{total_wan:.2f} 万</div>
    </div>
    """, unsafe_allow_html=True)

# 转化漏斗
st.markdown('<div class="section-header">📉 转化漏斗</div>', unsafe_allow_html=True)
if "最新跟进状态" in df_m.columns and not df_m.empty:
    assigned = df_m[valid_mask & (df_m["最新跟进状态"] != "未分配")].shape[0]
    followed = df_m[valid_mask & (~df_m["最新跟进状态"].isin(["未分配", "待查看", "待联系"]))].shape[0]
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

# 转化率趋势
st.markdown('<div class="section-header">📈 转化率趋势</div>', unsafe_allow_html=True)

def map_ratio(r):
    return r if r <= 1.0 else 1.0 + (r - 1.0) * 0.2

if not df_m.empty and "日期" in df_m and not df_m["日期"].isna().all():
    daily = df_m.groupby(df_m["日期"].dt.date).agg(
        总客资=("品牌", "count"),
        有效客资=("外呼状态", lambda x: x.isin(["高意向", "低意向", "无需外呼"]).sum())
    ).reset_index()
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
    if not df_o.empty:
        daily_order = df_o.groupby(df_o["日期"].dt.date).size().reset_index(name="成交数")
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

# 销售额分布（品牌、品类、运营中心）
st.markdown('<div class="section-header">💰 销售额分布</div>', unsafe_allow_html=True)
if df_o.empty:
    st.warning("当前筛选条件下无订单数据，无法展示销售额分布")
else:
    tab1, tab2, tab3 = st.tabs(["🏷️ 品牌", "📦 品类", "📍 运营中心"])
    with tab1:
        brand_sale = df_o.groupby("品牌")["订单金额"].sum().sort_values(ascending=False).head(10).reset_index()
        brand_sale["万元"] = brand_sale["订单金额"] / 10000
        fig1 = px.bar(brand_sale, x="品牌", y="万元", color="万元", color_continuous_scale="Blues",
                      title="品牌销售额 Top10（万元）", text="万元")
        fig1.update_traces(texttemplate='%{text:.1f}', textposition='outside')
        fig1.update_layout(plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig1, use_container_width=True)
    with tab2:
        cat_sale = df_o.groupby("品类")["订单金额"].sum().reset_index()
        cat_sale["万元"] = cat_sale["订单金额"] / 10000
        fig2 = px.pie(cat_sale, names="品类", values="万元", title="品类销售额占比",
                      color_discrete_sequence=px.colors.qualitative.Pastel)
        fig2.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig2, use_container_width=True)
    with tab3:
        center_sale = df_o.groupby("运营中心")["订单金额"].sum().reset_index()
        center_sale["万元"] = center_sale["订单金额"] / 10000
        fig3 = px.bar(center_sale, x="运营中心", y="万元", color="万元", color_continuous_scale="Tealgrn",
                      title="运营中心销售额（万元）")
        fig3.update_layout(xaxis_tickangle=-45, plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig3, use_container_width=True)

# ========== 订单金额热力省 & 热力城市 ==========
st.markdown('<div class="section-header">🗺️ 订单金额热力省 & 热力城市</div>', unsafe_allow_html=True)

if df_o.empty:
    st.info("暂无订单数据，无法绘制省份/城市销售额分布")
else:
    # 省份销售额
    province_sale = df_o.groupby("省份_订单")["订单金额"].sum().reset_index()
    province_sale = province_sale[province_sale["省份_订单"].notna() & (province_sale["省份_订单"] != "")]
    province_sale["万元"] = province_sale["订单金额"] / 10000
    province_sale_sorted = province_sale.sort_values("万元", ascending=False)
    
    # 城市销售额
    city_sale = df_o[df_o["城市_订单"] != ""].groupby("城市_订单")["订单金额"].sum().reset_index()
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

# ========== 客资数量热力省 & 热力城市 ==========
st.markdown('<div class="section-header">📊 客资数量热力省 & 热力城市</div>', unsafe_allow_html=True)

if df_m.empty:
    st.info("当前筛选条件下无客资数据")
else:
    # 客资省份统计
    province_leads = df_m.groupby("省份_客资").size().reset_index(name="客资数量")
    province_leads = province_leads[province_leads["省份_客资"].notna() & (province_leads["省份_客资"] != "")]
    province_leads_sorted = province_leads.sort_values("客资数量", ascending=False)
    
    # 客资城市统计
    city_leads = df_m[df_m["城市_客资"] != ""].groupby("城市_客资").size().reset_index(name="客资数量")
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
