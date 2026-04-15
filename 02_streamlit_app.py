"""
天猫新零售数据看板 - ECharts精美版
可直接部署到Streamlit Cloud，所有人可通过链接访问
"""
import streamlit as st
import pandas as pd
import sqlite3
import zipfile
import tempfile
import os
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from streamlit_echarts import st_echarts

st.set_page_config(layout="wide", page_title="天猫新零售数据看板", page_icon="📊")

# ==================== 精美样式 ====================
st.markdown("""
<style>
    .stApp {
        background: linear-gradient(135deg, #f0f4fc 0%, #e9eef6 100%);
        font-family: 'Inter', 'Segoe UI', 'Roboto', sans-serif;
    }
    /* 总客资 - 蓝色边框 */
    .metric-card-1 {
        background: linear-gradient(135deg, #ffffff 0%, #eff6ff 100%);
        border-radius: 24px;
        padding: 1rem 1.2rem;
        box-shadow: 0 4px 12px rgba(0,0,0,0.03);
        border: 1px solid rgba(59,130,246,0.15);
        border-left: 5px solid #3b82f6;
        transition: all 0.2s;
    }
    /* 有效客资 - 绿色边框 */
    .metric-card-2 {
        background: linear-gradient(135deg, #ffffff 0%, #f0fdf4 100%);
        border-radius: 24px;
        padding: 1rem 1.2rem;
        box-shadow: 0 4px 12px rgba(0,0,0,0.03);
        border: 1px solid rgba(16,185,129,0.15);
        border-left: 5px solid #10b981;
        transition: all 0.2s;
    }
    /* 成交单量 - 紫色边框 */
    .metric-card-3 {
        background: linear-gradient(135deg, #ffffff 0%, #faf5ff 100%);
        border-radius: 24px;
        padding: 1rem 1.2rem;
        box-shadow: 0 4px 12px rgba(0,0,0,0.03);
        border: 1px solid rgba(139,92,246,0.15);
        border-left: 5px solid #8b5cf6;
        transition: all 0.2s;
    }
    /* 总金额 - 橙色边框 */
    .metric-card-4 {
        background: linear-gradient(135deg, #ffffff 0%, #fffbeb 100%);
        border-radius: 24px;
        padding: 1rem 1.2rem;
        box-shadow: 0 4px 12px rgba(0,0,0,0.03);
        border: 1px solid rgba(245,158,11,0.15);
        border-left: 5px solid #f59e0b;
        transition: all 0.2s;
    }
    .metric-card-1:hover, .metric-card-2:hover,
    .metric-card-3:hover, .metric-card-4:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 20px rgba(0,0,0,0.06);
    }
    .metric-label {
        font-size: 0.8rem;
        font-weight: 600;
        color: #4b5563;
        letter-spacing: 0.03em;
        margin-bottom: 0.4rem;
    }
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        color: #111827;
        line-height: 1.2;
    }
    .metric-compare {
        font-size: 0.7rem;
        color: #6b7280;
        margin-top: 0.5rem;
        display: flex;
        gap: 0.8rem;
    }
    .compare-up { color: #10b981; font-weight: 500; }
    .compare-down { color: #ef4444; font-weight: 500; }
    .dashboard-title {
        font-size: 2.2rem;
        font-weight: 800;
        background: linear-gradient(120deg, #1e40af, #7c3aed, #ec4899);
        background-size: 200% auto;
        -webkit-background-clip: text;
        background-clip: text;
        color: transparent;
        animation: gradient_shift 3s ease infinite;
    }
    @keyframes gradient_shift {
        0% { background-position: 0% center; }
        50% { background-position: 100% center; }
        100% { background-position: 0% center; }
    }
    .section-header {
        font-size: 1.3rem;
        font-weight: 600;
        color: #1f2937;
        border-left: 5px solid;
        border-image: linear-gradient(180deg, #3b82f6, #7c3aed) 1;
        padding-left: 0.8rem;
        margin: 1.2rem 0 1rem 0;
    }
    .echarts-card {
        background: white;
        border-radius: 24px;
        padding: 1rem;
        box-shadow: 0 2px 8px rgba(0,0,0,0.02);
        border: 1px solid #eef2f6;
        margin-bottom: 1rem;
    }
    .divider {
        margin: 1.5rem 0;
        border: none;
        height: 2px;
        background: linear-gradient(90deg, transparent, #e2e8f0, transparent);
    }
    .badge {
        display: inline-block;
        padding: 0.2rem 0.6rem;
        border-radius: 20px;
        font-size: 0.7rem;
        font-weight: 600;
        margin-left: 0.5rem;
    }
    .badge-new { background: linear-gradient(135deg, #f093fb, #f5576c); color: white; }
    [data-testid="stSidebar"] {
        background: rgba(255,255,255,0.95);
        backdrop-filter: blur(8px);
        border-right: 1px solid #e2e8f0;
    }
</style>
""", unsafe_allow_html=True)

# ==================== 坐标数据 ====================
PROVINCE_CENTER = {
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

CITY_CENTER = {
    '杭州市': [120.1551, 30.2741], '宁波市': [121.5495, 29.8683],
    '温州市': [120.6998, 28.0006], '成都市': [104.0668, 30.5728],
    '南京市': [118.7969, 32.0603], '苏州市': [120.5853, 31.2990],
    '广州市': [113.2644, 23.1291], '深圳市': [114.0579, 22.5431],
    '武汉市': [114.3055, 30.5931], '长沙市': [112.9388, 28.2282],
    '郑州市': [113.6254, 34.7466], '青岛市': [120.3826, 36.0671],
    '济南市': [117.0009, 36.6758], '西安市': [108.9402, 34.3416],
    '福州市': [119.2965, 26.0745], '厦门市': [118.0894, 24.4798],
    '合肥市': [117.2272, 31.8206], '南昌市': [115.8582, 28.6820],
    '沈阳市': [123.4315, 41.8057], '大连市': [121.6147, 38.9140],
    '北京市': [116.4074, 39.9042], '上海市': [121.4737, 31.2304],
}

STANDARD_PROVINCES = set(PROVINCE_CENTER.keys())

# ==================== 辅助函数 ====================
def standardize_brand(val):
    if pd.isna(val): return "未知"
    s = str(val).strip().lower()
    if '小天鹅' in s or 'swan' in s: return "小天鹅"
    if '东芝' in s or 'toshiba' in s: return "东芝"
    if 'colmo' in s or '科摩' in s: return "colmo"
    if '美的' in s or 'midea' in s: return "美的"
    return str(val).strip()

def normalize_province(name):
    if not name or pd.isna(name): return None
    name = str(name).strip()
    if name in ['北京','北京市']: return '北京市'
    if name in ['上海','上海市']: return '上海市'
    if name in ['天津','天津市']: return '天津市'
    if name in ['重庆','重庆市']: return '重庆市'
    if name in ['广西','广西壮族自治区']: return '广西壮族自治区'
    if name in ['内蒙古','内蒙古自治区']: return '内蒙古自治区'
    if name in ['宁夏','宁夏回族自治区']: return '宁夏回族自治区'
    if name in ['新疆','新疆维吾尔自治区']: return '新疆维吾尔自治区'
    if name in ['西藏','西藏自治区']: return '西藏自治区'
    if name.endswith('省'): return name
    common = ['江苏','浙江','广东','山东','河南','四川','湖北','湖南','河北','福建','安徽','辽宁','江西','陕西','山西','云南','贵州','甘肃','青海','吉林','黑龙江','海南','台湾']
    if name in common: return name + '省'
    return name

def extract_province(raw):
    if pd.isna(raw) or not raw: return None
    s = str(raw).strip()
    if '-' in s:
        parts = s.split('-')
        province_part = parts[0].strip() if len(parts) == 2 else (parts[1].strip() if len(parts) >= 3 else s)
    else:
        province_part = s
    return normalize_province(province_part)

# ==================== 环比计算 ====================
def get_prev_day(start_date, end_date):
    days = (end_date - start_date).days
    prev_end = start_date - timedelta(days=1)
    prev_start = prev_end - timedelta(days=days)
    return prev_start, prev_end

def get_prev_month(start_date, end_date):
    days = (end_date - start_date).days
    prev_end = start_date - relativedelta(months=1)
    prev_start = prev_end - timedelta(days=days)
    return prev_start, prev_end

def fmt_change(current, prev):
    if prev is None or prev == 0:
        return '<span style="color:#94a3b8;">无数据</span>', None
    change = (current - prev) / prev
    arrow = "▲" if change >= 0 else "▼"
    cls = "compare-up" if change >= 0 else "compare-down"
    pct = f"{abs(change)*100:.1f}%"
    return f'<span class="{cls}">{arrow} {pct}</span>', change

def cmp_html(current, prev_day, prev_month):
    day_html, _ = fmt_change(current, prev_day)
    month_html, _ = fmt_change(current, prev_month)
    return f'<div class="metric-compare"><span>日环比 {day_html}</span><span>月环比 {month_html}</span></div>'

# ==================== ECharts配置函数 ====================
def ec_bar_line(title, x_data, series_dict, y_names, colors):
    series = []
    for i, (y_name, vals) in enumerate(series_dict.items()):
        if i == len(series_dict) - 1:
            series.append({
                "name": y_name, "type": "line", "yAxisIndex": 1,
                "data": vals, "smooth": True,
                "lineStyle": {"width": 3, "color": colors[i]},
                "itemStyle": {"color": colors[i]},
                "symbol": "circle", "symbolSize": 6
            })
        else:
            series.append({
                "name": y_name, "type": "bar",
                "data": vals,
                "itemStyle": {"color": colors[i], "borderRadius": [4, 4, 0, 0]}
            })
    return {
        "title": {"text": title, "left": "center", "textStyle": {"fontSize": 14, "fontWeight": "600", "color": "#1f2937"}},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}},
        "legend": {"top": 0, "data": list(y_names), "textStyle": {"color": "#6b7280"}},
        "grid": {"left": 50, "right": 30, "bottom": 30, "top": 45},
        "xAxis": {"type": "category", "data": x_data, "axisLabel": {"color": "#4a4e57", "rotate": 30 if len(x_data) > 6 else 0}},
        "yAxis": [
            {"type": "value", "axisLabel": {"color": "#606776"}, "splitLine": {"lineStyle": {"color": "#ebeef5"}}},
            {"type": "value", "axisLabel": {"color": "#606776", "formatter": "{value}%"}, "splitLine": {"show": False}}
        ],
        "series": series
    }

def ec_pie(title, data_dict, colors=None):
    if colors is None:
        colors = ['#5470c6', '#91cc75', '#fac858', '#ee6666', '#73c0de', '#3fb950', '#f7ba5e', '#9a60b4', '#4facfe', '#00f2fe']
    return {
        "title": {"text": title, "left": "center", "textStyle": {"fontSize": 14, "fontWeight": "600", "color": "#1f2937"}},
        "tooltip": {"trigger": "item", "formatter": "{b}: {c} ({d}%)"},
        "legend": {"bottom": 5, "textStyle": {"color": "#6b7280"}},
        "color": colors,
        "series": [{
            "type": "pie", "radius": ["40%", "70%"],
            "avoidLabelOverlap": False,
            "itemStyle": {"borderRadius": 8, "borderColor": "#fff", "borderWidth": 2},
            "label": {"show": False},
            "emphasis": {"label": {"show": True, "fontSize": 13, "fontWeight": "bold"}},
            "labelLine": {"show": False},
            "data": [{"value": int(v), "name": k} for k, v in data_dict.items()]
        }]
    }

def ec_bar_h(title, data_dict, value_unit="", color_start="#4a90e2", color_end="#7c3aed"):
    sorted_items = sorted(data_dict.items(), key=lambda x: x[1], reverse=True)[:15]
    cats = [item[0] for item in sorted_items]
    vals = [float(item[1]) for item in sorted_items]
    return {
        "title": {"text": title, "left": "center", "textStyle": {"fontSize": 14, "fontWeight": "600", "color": "#1f2937"}},
        "tooltip": {"trigger": "axis", "axisPointer": {"type": "shadow"}, "formatter": f"{{b}}: {{c}}{value_unit}"},
        "grid": {"left": 120, "right": 60, "bottom": 20, "top": 30},
        "xAxis": {"type": "value", "axisLabel": {"color": "#606776"}, "splitLine": {"lineStyle": {"color": "#ebeef5"}}},
        "yAxis": {"type": "category", "data": list(reversed(cats)), "axisLabel": {"color": "#4a4e57", "fontSize": 11}},
        "series": [{
            "type": "bar", "data": vals, "barMaxWidth": 24,
            "itemStyle": {
                "color": {"type": "linear", "x": 0, "y": 0, "x2": 1, "y2": 0,
                          "colorStops": [{"offset": 0, "color": color_start}, {"offset": 1, "color": color_end}]},
                "borderRadius": [0, 4, 4, 0]
            }
        }]
    }

def ec_funnel(title, data_list, colors=None):
    if colors is None:
        colors = ['#4a90e2', '#73c0de', '#3fb950', '#f7ba5e', '#ee6666']
    return {
        "title": {"text": title, "left": "center", "textStyle": {"fontSize": 14, "fontWeight": "600", "color": "#1f2937"}},
        "tooltip": {"trigger": "item", "formatter": "{b}: {c}"},
        "legend": {"bottom": 5, "textStyle": {"color": "#6b7280"}},
        "color": colors,
        "series": [{
            "type": "funnel", "left": "10%", "top": 30, "bottom": 50, "width": "80%",
            "min": 0, "max": data_list[0][1] if data_list else 1,
            "minSize": "0%", "maxSize": "100%", "gap": 3,
            "itemStyle": {"borderColor": "#fff", "borderWidth": 2, "borderRadius": 4},
            "label": {"show": True, "position": "inside", "formatter": "{b}\n{c}", "fontSize": 12},
            "data": [{"value": int(v), "name": n} for n, v in data_list]
        }]
    }

def ec_map(title, data_dict):
    geo_coord = {**PROVINCE_CENTER, **CITY_CENTER}
    scatter_data = []
    for name, val in data_dict.items():
        if name in geo_coord:
            scatter_data.append({
                "name": name,
                "value": geo_coord[name] + [val.get("客资", 0), val.get("转化率", 0)]
            })
    return {
        "title": {"text": title, "left": "center", "textStyle": {"fontSize": 14, "fontWeight": "600", "color": "#1f2937"}},
        "tooltip": {
            "trigger": "item",
            "formatter": lambda p: f"{p.name}<br/>客资数: {p.value[2] if len(p.value)>2 else '-'}<br/>转化率: {p.value[3] if len(p.value)>3 else '-'}%" if len(p.value) > 2 else p.name
        },
        "geo": {
            "map": "china", "roam": True, "zoom": 1.2,
            "itemStyle": {"areaColor": "#e8f4ff", "borderColor": "#a0c4e8", "borderWidth": 1},
            "emphasis": {"itemStyle": {"areaColor": "#c8e6ff"}},
            "label": {"show": False}
        },
        "series": [{
            "type": "effectScatter", "coordinateSystem": "geo",
            "data": scatter_data,
            "symbolSize": lambda v: max(8, min(40, v[2] / 100)),
            "itemStyle": {
                "color": {"type": "radial", "x": 0.5, "y": 0.5, "r": 0.5,
                          "colorStops": [{"offset": 0, "color": "#27ae60"}, {"offset": 1, "color": "#0d6e3f"}]}
            },
            "emphasis": {"scale": 1.5},
            "rippleEffect": {"brushType": "stroke", "scale": 3}
        }]
    }

# ==================== 数据加载 ====================
@st.cache_data(ttl=3600)
def load_data():
    if not os.path.exists("data.zip"):
        st.error("❌ 未找到 data.zip 文件！请先运行 01_prepare_data.py 生成数据文件")
        st.stop()
    with zipfile.ZipFile("data.zip", "r") as zf:
        db_files = [f for f in zf.namelist() if f.endswith(".db")]
        if not db_files:
            st.error("❌ data.zip 中未找到 .db 文件")
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

    # ========== 客资表标准化 ==========
    if "获取时间" in df_main.columns:
        df_main["日期"] = pd.to_datetime(df_main["获取时间"], errors="coerce")
    elif "日期" in df_main.columns:
        df_main["日期"] = pd.to_datetime(df_main["日期"], errors="coerce")
    else:
        df_main["日期"] = pd.NaT

    if "运营中心" in df_main.columns:
        df_main["运中"] = df_main["运营中心"].fillna("未知")
    else:
        df_main["运中"] = df_main.get("运中", pd.Series(["未知"]*len(df_main))).fillna("未知")

    brand_col = next((c for c in ["意向品牌", "品牌"] if c in df_main.columns), None)
    df_main["品牌"] = df_main[brand_col].apply(standardize_brand) if brand_col else "未知"
    df_main["品类"] = df_main.get("品类", pd.Series(["未知"]*len(df_main))).fillna("未知")

    if "外呼状态" not in df_main.columns:
        df_main["外呼状态"] = ""
    if "最新跟进状态" not in df_main.columns:
        df_main["最新跟进状态"] = ""

    # ========== 订单表标准化 ==========
    if "日期" in df_order.columns:
        df_order["日期"] = pd.to_datetime(df_order["日期"], errors="coerce")
    else:
        df_order["日期"] = pd.NaT

    df_order["运中"] = df_order.get("运中", df_order.get("运营中心", pd.Series(["未知"]*len(df_order)))).fillna("未知")
    brand_col2 = next((c for c in ["品牌", "意向品牌"] if c in df_order.columns), None)
    df_order["品牌"] = df_order[brand_col2].apply(standardize_brand) if brand_col2 else "未知"
    df_order["品类"] = df_order.get("品类", pd.Series(["未知"]*len(df_order))).fillna("未知")

    if "订单金额" in df_order.columns:
        df_order["订单金额"] = pd.to_numeric(df_order["订单金额"], errors="coerce").fillna(0)
    else:
        df_order["订单金额"] = 0.0

    # 省份城市
    for col in ["省份", "省市"]:
        if col in df_main.columns:
            df_main["省份_raw"] = df_main[col].fillna("").astype(str).str.strip()
            break
    else:
        df_main["省份_raw"] = ""
    for col in ["城市", "市区"]:
        if col in df_main.columns:
            df_main["城市_raw"] = df_main[col].fillna("").astype(str).str.strip()
            break
    else:
        df_main["城市_raw"] = ""
    for col in ["省份", "省市"]:
        if col in df_order.columns:
            df_order["省份_raw"] = df_order[col].fillna("").astype(str).str.strip()
            break
    else:
        df_order["省份_raw"] = ""
    for col in ["城市", "市区"]:
        if col in df_order.columns:
            df_order["城市_raw"] = df_order[col].fillna("").astype(str).str.strip()
            break
    else:
        df_order["城市_raw"] = ""

    df_main["省份_标准化"] = df_main["省份_raw"].apply(extract_province)
    df_order["省份_标准化"] = df_order["省份_raw"].apply(extract_province)

    return df_main, df_order

# ==================== 主程序 ====================
df_main, df_order = load_data()
if df_main.empty:
    st.error("客资明细表为空")
    st.stop()

# 筛选器
st.sidebar.markdown("## 🎛️ 筛选面板")
if not df_main["日期"].isna().all():
    min_date = df_main["日期"].min().date()
    max_date = df_main["日期"].max().date()
else:
    min_date = datetime.today().date()
    max_date = datetime.today().date()

start_date = st.sidebar.date_input("📅 开始日期", min_date)
end_date = st.sidebar.date_input("📅 结束日期", max_date)

all_brands = sorted(set(df_main["品牌"].dropna().unique()) | set(df_order["品牌"].dropna().unique()))
all_brands = [b for b in all_brands if b and b != "未知"]
all_cats = sorted([c for c in df_main["品类"].dropna().unique() if c and c != "未知"])
all_centers = sorted([c for c in df_main["运中"].dropna().unique() if c and c != "未知"])
sel_brand = st.sidebar.multiselect("🏷️ 品牌", all_brands, default=[])
sel_cat = st.sidebar.multiselect("📦 品类", all_cats, default=[])
sel_center = st.sidebar.multiselect("📍 运营中心", all_centers, default=[])

# 应用筛选
def apply_filters(dm, do):
    dm2 = dm.copy()
    do2 = do.copy()
    if not dm2["日期"].isna().all():
        dm2 = dm2[(dm2["日期"].dt.date >= start_date) & (dm2["日期"].dt.date <= end_date)]
        do2 = do2[(do2["日期"].dt.date >= start_date) & (do2["日期"].dt.date <= end_date)]
    if sel_brand:
        dm2 = dm2[dm2["品牌"].isin(sel_brand)]
        do2 = do2[do2["品牌"].isin(sel_brand)]
    if sel_cat:
        dm2 = dm2[dm2["品类"].isin(sel_cat)]
        do2 = do2[do2["品类"].isin(sel_cat)]
    if sel_center:
        dm2 = dm2[dm2["运中"].isin(sel_center)]
        do2 = do2[do2["运中"].isin(sel_center)]
    return dm2, do2

df_m, df_o = apply_filters(df_main, df_order)

# ========== KPI计算（与你的代码逻辑一致） ==========
total_leads = len(df_m)
valid_mask = df_m["外呼状态"].isin(["高意向", "低意向", "无需外呼"])
valid_leads = int(valid_mask.sum())
assigned = int(df_m[valid_mask & (df_m["最新跟进状态"] != "未分配")].shape[0]) if "最新跟进状态" in df_m.columns else 0
followed = int(df_m[valid_mask & (~df_m["最新跟进状态"].isin(["未分配", "待查看", "待联系"]))].shape[0]) if "最新跟进状态" in df_m.columns else 0
order_count = len(df_o)
total_amount = float(df_o["订单金额"].sum()) if not df_o.empty else 0.0

# 环比计算
dps_start, dps_end = get_prev_day(start_date, end_date)
mps_start, mps_end = get_prev_month(start_date, end_date)

def filter_df(dm, do, s, e):
    dm2 = dm.copy(); do2 = do.copy()
    if not dm2["日期"].isna().all():
        dm2 = dm2[(dm2["日期"].dt.date >= s) & (dm2["日期"].dt.date <= e)]
        do2 = do2[(do2["日期"].dt.date >= s) & (do2["日期"].dt.date <= e)]
    if sel_brand:
        dm2 = dm2[dm2["品牌"].isin(sel_brand)]; do2 = do2[do2["品牌"].isin(sel_brand)]
    if sel_cat:
        dm2 = dm2[dm2["品类"].isin(sel_cat)]; do2 = do2[do2["品类"].isin(sel_cat)]
    if sel_center:
        dm2 = dm2[dm2["运中"].isin(sel_center)]; do2 = do2[do2["运中"].isin(sel_center)]
    return dm2, do2

dm_d, do_d = filter_df(df_main, df_order, dps_start, dps_end)
dm_m, do_m = filter_df(df_main, df_order, mps_start, mps_end)

tl_d = len(dm_d); vl_d = int(dm_d["外呼状态"].isin(["高意向","低意向","无需外呼"]).sum()); oc_d = len(do_d); ta_d = float(do_d["订单金额"].sum()) if not do_d.empty else 0.0
tl_m = len(dm_m); vl_m = int(dm_m["外呼状态"].isin(["高意向","低意向","无需外呼"]).sum()); oc_m = len(do_m); ta_m = float(do_m["订单金额"].sum()) if not do_m.empty else 0.0

c1, c2, c3, c4 = st.columns(4)
latest = max_date.strftime("%Y年%m月%d日")
st.markdown(f'<div class="dashboard-title">🏬 天猫新零售数据看板</div>', unsafe_allow_html=True)
st.markdown(f"<div style='color:#4b5563; margin-bottom:1rem;'>数据更新至 {latest}</div>", unsafe_allow_html=True)

with c1:
    st.markdown(f"""<div class="metric-card-1"><div class="metric-label">📋 总客资</div><div class="metric-value">{total_leads:,}</div>{cmp_html(total_leads, tl_d, tl_m)}</div>""", unsafe_allow_html=True)
with c2:
    st.markdown(f"""<div class="metric-card-2"><div class="metric-label">✅ 有效客资</div><div class="metric-value">{valid_leads:,}</div>{cmp_html(valid_leads, vl_d, vl_m)}</div>""", unsafe_allow_html=True)
with c3:
    st.markdown(f"""<div class="metric-card-3"><div class="metric-label">🛒 成交单量</div><div class="metric-value">{order_count:,}</div>{cmp_html(order_count, oc_d, oc_m)}</div>""", unsafe_allow_html=True)
with c4:
    st.markdown(f"""<div class="metric-card-4"><div class="metric-label">💰 总金额（万元）</div><div class="metric-value">{total_amount/10000:.2f} 万</div>{cmp_html(total_amount, ta_d, ta_m)}</div>""", unsafe_allow_html=True)

# ==================== ECharts 图表区 ====================
st.markdown('<hr class="divider">', unsafe_allow_html=True)
st.markdown('### 🎨 ECharts 精美图表 <span class="badge badge-new">增强版</span>', unsafe_allow_html=True)

# 转化漏斗（使用你的计算逻辑）
st.markdown('<div class="echarts-card">', unsafe_allow_html=True)
st.markdown('<div class="section-header">📉 客户转化漏斗</div>', unsafe_allow_html=True)
st_echarts(ec_funnel("客户转化漏斗", [
    ("总客资", total_leads),
    ("有效客资", valid_leads),
    ("已分配", assigned),
    ("已跟进", followed),
    ("成交", order_count)
]), height="400px")
st.markdown('</div>', unsafe_allow_html=True)

# 月度趋势
st.markdown('<div class="section-header">📈 月度客资与订单趋势</div>', unsafe_allow_html=True)
if not df_m.empty and not df_o.empty:
    df_m["年月"] = df_m["日期"].dt.to_period("M").astype(str)
    df_o["年月"] = df_o["日期"].dt.to_period("M").astype(str)
    months = sorted(set(df_m["年月"]) | set(df_o["年月"]))
    leads_v = [int(df_m[df_m["年月"]==m].shape[0]) for m in months]
    orders_v = [int(df_o[df_o["年月"]==m].shape[0]) for m in months]
    conv_v = [round(orders_v[i]/leads_v[i]*100,2) if leads_v[i]>0 else 0 for i in range(len(months))]
    st_echarts(ec_bar_line("月度客资数与订单数趋势", months,
        {"客资数": leads_v, "订单数": orders_v, "转化率(%)": conv_v},
        ["客资数", "订单数", "转化率(%)"], ["#4a90e2", "#27ae60", "#e07050"]), height="420px")

# 日转化率面积图
st.markdown('<div class="section-header">📈 日转化率趋势（面积图）</div>', unsafe_allow_html=True)
if not df_m.empty and not df_o.empty:
    # 过滤掉NaT日期
    df_m_clean = df_m.dropna(subset=["日期"])
    daily_m = df_m_clean.groupby(df_m_clean["日期"].dt.date).agg(
        总客资=("品牌", "count"),
        有效客资=("外呼状态", lambda x: x.isin(["高意向","低意向","无需外呼"]).sum())
    ).reset_index()
    daily_o = df_o.dropna(subset=["日期"]).groupby(df_o["日期"].dt.date).size().reset_index(name="成交数")
    daily_m = daily_m.merge(daily_o, on="日期", how="left")
    daily_m["成交数"] = pd.to_numeric(daily_m["成交数"], errors="coerce").fillna(0)
    daily_m["有效客资"] = pd.to_numeric(daily_m["有效客资"], errors="coerce").fillna(0)
    daily_m["转化率"] = daily_m.apply(lambda r: round(r["成交数"]/r["有效客资"]*100,2) if r["有效客资"]>0 else 0, axis=1)
    dates_str = [str(d) for d in daily_m["日期"]]
    conv_area = {
        "title": {"text": "每日转化率趋势", "left": "center", "textStyle": {"fontSize": 14, "fontWeight": "600", "color": "#1f2937"}},
        "tooltip": {"trigger": "axis"},
        "grid": {"left": 50, "right": 30, "bottom": 30, "top": 45},
        "xAxis": {"type": "category", "data": dates_str, "axisLabel": {"color": "#4a4e57", "rotate": 30}},
        "yAxis": {"type": "value", "axisLabel": {"color": "#606776", "formatter": "{value}%"}, "splitLine": {"lineStyle": {"color": "#ebeef5"}}},
        "series": [{"name": "转化率", "type": "line", "data": [float(v) for v in daily_m["转化率"]], "smooth": True,
                   "lineStyle": {"width": 2, "color": "#ef4444"},
                   "itemStyle": {"color": "#ef4444"},
                   "areaStyle": {"color": {"type": "linear", "x": 0, "y": 0, "x2": 0, "y2": 1,
                                   "colorStops": [{"offset": 0, "color": "rgba(239,68,68,0.3)"}, {"offset": 1, "color": "rgba(239,68,68,0.05)"}]}},
                   "symbol": "circle", "symbolSize": 4}]
    }
    st_echarts(conv_area, height="450px")

# 品类饼图
col1, col2 = st.columns(2)
with col1:
    st.markdown('<div class="echarts-card">', unsafe_allow_html=True)
    if not df_m.empty:
        st_echarts(ec_pie("品类客资量占比", df_m.groupby("品类").size().to_dict()), height="360px")
    st.markdown('</div>', unsafe_allow_html=True)
with col2:
    st.markdown('<div class="echarts-card">', unsafe_allow_html=True)
    if not df_o.empty:
        st_echarts(ec_pie("品类订单金额占比", df_o.groupby("品类")["订单金额"].sum().to_dict(),
                          ['#667eea','#764ba2','#f093fb','#f5576c','#4facfe','#00f2fe']), height="360px")
    st.markdown('</div>', unsafe_allow_html=True)

# 商品类目 TOP10 分析
st.markdown('<div class="echarts-card">', unsafe_allow_html=True)
st.markdown('<div class="section-header">🏷️ 商品类目 TOP10（按订单金额）</div>', unsafe_allow_html=True)
if not df_o.empty:
    cat_top10 = df_o.groupby("商品类目")["订单金额"].sum().sort_values(ascending=False).head(10)
    st_echarts(ec_bar_h("商品类目 TOP10", cat_top10.to_dict(), "万元", "#f7ba5e", "#f97316"), height="400px")
st.markdown('</div>', unsafe_allow_html=True)

# 运营中心分析
col3, col4 = st.columns(2)
with col3:
    st.markdown('<div class="echarts-card">', unsafe_allow_html=True)
    if not df_m.empty:
        st_echarts(ec_bar_h("运营中心客资量 TOP15", df_m.groupby("运中").size().to_dict(), "客资", "#4a90e2", "#7c3aed"), height="400px")
    st.markdown('</div>', unsafe_allow_html=True)
with col4:
    st.markdown('<div class="echarts-card">', unsafe_allow_html=True)
    if not df_m.empty and not df_o.empty:
        cl = df_m.groupby("运中").size()
        co = df_o.groupby("运中").size()
        conv = {c: round(co.get(c,0)/cl[c]*100,2) if cl[c]>0 else 0 for c in cl.index}
        conv_s = dict(sorted(conv.items(), key=lambda x: x[1], reverse=True)[:15])
        st_echarts(ec_bar_h("运营中心转化率 TOP15", conv_s, "%", "#27ae60", "#10b981"), height="400px")
    st.markdown('</div>', unsafe_allow_html=True)

# 品牌分析
col5, col6 = st.columns(2)
with col5:
    st.markdown('<div class="echarts-card">', unsafe_allow_html=True)
    if not df_m.empty:
        st_echarts(ec_bar_h("品牌客资量 TOP15", df_m.groupby("品牌").size().to_dict(), "客资", "#f093fb", "#f5576c"), height="400px")
    st.markdown('</div>', unsafe_allow_html=True)
with col6:
    st.markdown('<div class="echarts-card">', unsafe_allow_html=True)
    if not df_m.empty and not df_o.empty:
        bl = df_m.groupby("品牌").size()
        bo = df_o.groupby("品牌").size()
        bconv = {b: round(bo.get(b,0)/bl[b]*100,2) if bl[b]>0 else 0 for b in bl.index}
        bconv_s = dict(sorted(bconv.items(), key=lambda x: x[1], reverse=True)[:15])
        st_echarts(ec_bar_h("品牌转化率 TOP15", bconv_s, "%", "#e07050", "#ee6666"), height="400px")
    st.markdown('</div>', unsafe_allow_html=True)

# 省份销售额 TOP20
st.markdown('<div class="echarts-card">', unsafe_allow_html=True)
st.markdown('<div class="section-header">🗺️ 省份销售额排行 TOP20</div>', unsafe_allow_html=True)
if not df_o.empty:
    prov = df_o.groupby("省份_标准化")["订单金额"].sum()
    prov = prov[prov.index.isin(STANDARD_PROVINCES)].dropna()
    prov_wan = (prov / 10000).round(1).sort_values(ascending=False).head(20).to_dict()
    st_echarts(ec_bar_h("省份销售额(万元) TOP20", prov_wan, "万元", "#4facfe", "#00f2fe"), height="500px")
st.markdown('</div>', unsafe_allow_html=True)

# 城市销售额 TOP15
st.markdown('<div class="echarts-card">', unsafe_allow_html=True)
st.markdown('<div class="section-header">🏙️ 城市销售额 TOP15</div>', unsafe_allow_html=True)
if not df_o.empty:
    city = df_o[df_o["城市_raw"]!=""].groupby("城市_raw")["订单金额"].sum()
    city = (city/10000).round(1).sort_values(ascending=False).head(15).to_dict()
    st_echarts(ec_bar_h("城市销售额(万元) TOP15", city, "万元", "#f7ba5e", "#f093fb"), height="400px")
st.markdown('</div>', unsafe_allow_html=True)

# 全国热力地图
st.markdown('<div class="echarts-card">', unsafe_allow_html=True)
st.markdown('<div class="section-header">🌍 全国运营中心分布热力图</div>', unsafe_allow_html=True)
if not df_m.empty and not df_o.empty:
    cl = df_m.groupby("运中").size()
    co = df_o.groupby("运中").size()
    geo_data = {}
    for c in cl.index:
        if c in CITY_CENTER:
            geo_data[c] = {"客资": int(cl.get(c,0)), "转化率": round(co.get(c,0)/cl[c]*100,2) if cl[c]>0 else 0}
    if geo_data:
        st_echarts(ec_map("全国运营中心（气泡大小=客资数，颜色=转化率）", geo_data), height="500px")
    else:
        st.info("无有效的运营中心地理位置数据")
else:
    st.info("无数据")
st.markdown('</div>', unsafe_allow_html=True)

# 运营中心汇总明细表
st.markdown('<div class="echarts-card">', unsafe_allow_html=True)
st.markdown('<div class="section-header">📊 运营中心汇总明细</div>', unsafe_allow_html=True)
if not df_m.empty and not df_o.empty:
    cl = df_m.groupby("运中").size()
    co = df_o.groupby("运中").size()
    ca = df_o.groupby("运中")["订单金额"].sum()
    summary = pd.DataFrame({
        "运营中心": cl.index,
        "客资数": cl.values,
        "订单数": [co.get(c, 0) for c in cl.index],
        "订单金额(万)": [round(ca.get(c, 0)/10000, 2) for c in cl.index]
    })
    summary["转化率"] = (summary["订单数"] / summary["客资数"].replace(0,1) * 100).round(2)
    summary = summary.sort_values("客资数", ascending=False)
    st.dataframe(summary, use_container_width=True, hide_index=True)
st.markdown('</div>', unsafe_allow_html=True)

# 品牌汇总明细表
st.markdown('<div class="echarts-card">', unsafe_allow_html=True)
st.markdown('<div class="section-header">🏷️ 品牌汇总明细</div>', unsafe_allow_html=True)
if not df_m.empty and not df_o.empty:
    bl = df_m.groupby("品牌").size()
    bo = df_o.groupby("品牌").size()
    ba = df_o.groupby("品牌")["订单金额"].sum()
    summary_b = pd.DataFrame({
        "品牌": bl.index,
        "客资数": bl.values,
        "订单数": [bo.get(b, 0) for b in bl.index],
        "订单金额(万)": [round(ba.get(b, 0)/10000, 2) for b in bl.index]
    })
    summary_b["转化率"] = (summary_b["订单数"] / summary_b["客资数"].replace(0,1) * 100).round(2)
    summary_b = summary_b.sort_values("客资数", ascending=False)
    st.dataframe(summary_b, use_container_width=True, hide_index=True)
st.markdown('</div>', unsafe_allow_html=True)

# 页脚
st.markdown("---")
st.markdown("""
<div style="text-align:center; padding:1.5rem; background:linear-gradient(135deg, #667eea 0%, #764ba2 100%);
     border-radius:20px; color:white; margin-top:2rem;">
    <div style="font-size:1.1rem; font-weight:600;">📊 天猫新零售数据看板 · ECharts精美增强版</div>
    <div style="font-size:0.85rem; opacity:0.9; margin-top:0.3rem;">数据驱动决策 · 整合客资与订单全链路分析</div>
</div>
""", unsafe_allow_html=True)
