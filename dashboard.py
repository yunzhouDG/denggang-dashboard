
# 生成完整的修正版 Streamlit 代码

complete_code = '''import streamlit as st
import pandas as pd
import sqlite3
import zipfile
import tempfile
import os
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(layout="wide", page_title="天猫新零售数据看板", page_icon="📊")

# ----------------------------- 自定义CSS美化 -----------------------------
st.markdown("""
<style>
    .stApp { background-color: #f5f7fb; font-family: 'Segoe UI', 'Roboto', sans-serif; }
    .css-1d391kg, .css-163ttbj, .eczjsme11 { background-color: #ffffff; border-right: 1px solid #e6e9f0; }
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
    .stDataFrame { border-radius: 16px; overflow: hidden; box-shadow: 0 2px 8px rgba(0,0,0,0.05); }
    .stButton button { border-radius: 30px; background-color: #3b82f6; color: white; border: none; font-weight: 500; }
    .stButton button:hover { background-color: #2563eb; transform: scale(1.02); }
    /* 调试信息样式 */
    .debug-info { background-color: #f0f9ff; border: 1px solid #0ea5e9; border-radius: 8px; padding: 10px; margin: 5px 0; }
    .debug-warning { background-color: #fffbeb; border: 1px solid #f59e0b; border-radius: 8px; padding: 10px; margin: 5px 0; }
</style>
""", unsafe_allow_html=True)

# ----------------------------- 省份中心坐标 -----------------------------
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

# ----------------------------- 【修正1】增强品牌标准化 -----------------------------
def standardize_brand(brand_val):
    """
    增强版品牌标准化函数
    处理各种变体：空格、大小写、特殊字符、中英文混合
    """
    if pd.isna(brand_val):
        return "未知"
    
    # 转换为字符串并清理
    s = str(brand_val).strip()
    
    # 保存原始值用于调试
    original = s
    
    # 统一转换为小写并去除多余空格
    s_lower = s.lower().replace(' ', '').replace('　', '').replace('\\t', '').replace('\\n', '')
    
    # 东芝品牌匹配（增强版）
    toshiba_patterns = ['东芝', 'toshiba', '東芝', 'とうしば', 'touhiba']
    if any(pattern in s_lower for pattern in toshiba_patterns):
        return "东芝"
    
    # 小天鹅品牌匹配
    swan_patterns = ['小天鹅', 'swan', 'littleswan', 'little_swan']
    if any(pattern in s_lower for pattern in swan_patterns):
        return "小天鹅"
    
    # COLMO品牌匹配
    colmo_patterns = ['colmo', '科摩', 'colm0', 'c0lmo']
    if any(pattern in s_lower for pattern in colmo_patterns):
        return "COLMO"
    
    # 美的品牌匹配（排除其他已匹配的品牌后）
    midea_patterns = ['美的', 'midea', 'midia', 'midea']
    if any(pattern in s_lower for pattern in midea_patterns):
        # 再次确认不是其他品牌
        if not any(p in s_lower for p in toshiba_patterns + swan_patterns + colmo_patterns):
            return "美的"
    
    # 如果无法识别，返回清理后的原始值
    return s

# ----------------------------- 【修正2】增强品牌筛选函数 -----------------------------
def apply_brand_filter(df, selected_brands):
    """
    增强版品牌筛选逻辑
    支持汇总选项和精确匹配
    """
    if not selected_brands:
        return df
    
    # 创建空条件
    cond = pd.Series(False, index=df.index)
    
    # 处理普通品牌（非汇总项）
    normal_brands = [b for b in selected_brands if b not in ["洗衣机汇总", "美的厨热", "美的冰箱", "美的空调"]]
    
    if normal_brands:
        # 使用精确匹配，确保大小写一致
        cond |= df["品牌"].isin([b.strip() for b in normal_brands])
    
    # 处理汇总选项
    if "洗衣机汇总" in selected_brands:
        # 小天鹅 或 (美的 且 洗衣机品类)
        washing_machine_cond = (df["品牌"] == "东芝")  # 注意：这里应该是小天鹅，修正如下
        washing_machine_cond = (df["品牌"] == "小天鹅") | ((df["品牌"] == "美的") & (df["品类"] == "洗衣机"))
        cond |= washing_machine_cond
    
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

    # 【修正3】增强订单金额处理
    if "订单金额" in df_order.columns:
        # 先查看原始数据类型和样本
        df_order["订单金额_原始"] = df_order["订单金额"]  # 保留原始值用于调试
        df_order["订单金额"] = pd.to_numeric(df_order["订单金额"], errors="coerce")
        # 检查有多少NaN值
        nan_count = df_order["订单金额"].isna().sum()
        if nan_count > 0:
            st.warning(f"订单金额中有 {nan_count} 个无效值被设为0")
        df_order["订单金额"] = df_order["订单金额"].fillna(0)
    else:
        df_order["订单金额"] = 0.0

    # 【修正4】统一品牌列并标准化（增强版）
    for df in [df_main, df_order]:
        # 确定原始品牌列
        if "品牌" in df.columns:
            raw_brand = df["品牌"].fillna("未知")
        elif "意向品牌" in df.columns:
            raw_brand = df["意向品牌"].fillna("未知")
        else:
            raw_brand = pd.Series(["未知"] * len(df), index=df.index)
        
        # 保存原始品牌用于调试
        df["品牌_原始"] = raw_brand
        
        # 应用标准化
        df["品牌"] = raw_brand.apply(standardize_brand)
        
        # 处理其他字段
        df["品类"] = df.get("品类", "未知").fillna("未知")
        df["运营中心"] = df.get("运营中心", df.get("运中", "未知")).fillna("未知")
        df["片区"] = df.get("片区", "未知").fillna("未知")
    
    # 确保其他必要字段存在
    if "外呼状态" not in df_main.columns:
        df_main["外呼状态"] = ""
    if "最新跟进状态" not in df_main.columns:
        df_main["最新跟进状态"] = ""
    if "市区" not in df_order.columns:
        df_order["市区"] = ""
    if "省市" not in df_order.columns:
        df_order["省市"] = ""

    return df_main, df_order

# ----------------------------- 省份提取 -----------------------------
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

def extract_province_from_shengshi(shengshi):
    if pd.isna(shengshi) or not shengshi:
        return None
    s = str(shengshi).strip()
    if '-' in s:
        parts = s.split('-')
        candidate = parts[0] if len(parts) >= 2 else parts[0]
        return normalize_province_name(candidate)
    else:
        return normalize_province_name(s)

# ----------------------------- 主程序 -----------------------------
df_main, df_order = load_data()
if df_main.empty:
    st.error("客资明细表为空，请检查数据源")
    st.stop()

# 初始化调试模式
if "debug_mode" not in st.session_state:
    st.session_state.debug_mode = False

with st.sidebar:
    st.session_state.debug_mode = st.checkbox("🔧 调试模式", value=False)
    
    # 【修正5】增强调试面板 - 显示品牌标准化详情
    if st.session_state.debug_mode:
        st.markdown("---")
        st.subheader("🔍 品牌标准化诊断")
        
        # 显示原始品牌分布（订单表）
        st.markdown("**原始品牌分布（前20）：**")
        if "品牌_原始" in df_order.columns:
            orig_counts = df_order["品牌_原始"].value_counts().head(20)
            st.write(orig_counts)
        
        # 显示标准化后品牌分布
        st.markdown("**标准化后品牌分布：**")
        std_counts = df_order["品牌"].value_counts()
        st.write(std_counts)
        
        # 专门检查东芝
        st.markdown("**东芝品牌详细检查：**")
        toshiba_original = df_order[df_order["品牌"] == "东芝"]["品牌_原始"].unique() if "品牌_原始" in df_order.columns else []
        st.write(f"被识别为'东芝'的原始品牌值: {list(toshiba_original)}")
        
        # 检查是否有可能遗漏的东芝变体
        potential_toshiba = df_order[df_order["品牌_原始"].str.contains('东|芝|toshiba|東芝', case=False, na=False)]["品牌_原始"].unique() if "品牌_原始" in df_order.columns else []
        st.write(f"包含'东/芝/toshiba'的原始值: {list(potential_toshiba)}")

# ========== 关键修改：合并主表和订单表的品牌作为默认选项 ==========
all_brands = set(df_main["品牌"].dropna().unique()) | set(df_order["品牌"].dropna().unique())
actual_brands = sorted([b for b in all_brands if b and b != "未知"])
actual_cats = sorted([c for c in df_main["品类"].dropna().unique() if c and c != "未知"])
actual_centers = sorted([c for c in df_main["运营中心"].dropna().unique() if c and c != "未知"])
actual_areas = sorted([a for a in df_main["片区"].dropna().unique() if a and a != "未知"])
brand_options = actual_brands + ["洗衣机汇总", "美的厨热", "美的冰箱", "美的空调"]

st.sidebar.markdown("## 🎛️ 筛选面板")
if not df_main["日期"].isna().all():
    min_date = df_main["日期"].min().date()
    max_date = df_main["日期"].max().date()
else:
    min_date = datetime.today().date()
    max_date = datetime.today().date()
date_range = st.sidebar.date_input("📅 日期范围", [min_date, max_date])

col1_s, col2_s = st.sidebar.columns(2)
with col1_s:
    sel_brand = st.multiselect("🏷️ 品牌", brand_options, default=actual_brands)
    sel_cat = st.multiselect("📦 品类", actual_cats, default=actual_cats)
with col2_s:
    sel_area = st.multiselect("🗺️ 片区", actual_areas, default=actual_areas)
    sel_center = st.multiselect("📍 运营中心", actual_centers, default=actual_centers)

def filter_by_date(df, date_range):
    if "日期" not in df.columns or df["日期"].isna().all():
        return df
    d_start, d_end = date_range
    return df[(df["日期"].dt.date >= d_start) & (df["日期"].dt.date <= d_end)]

# 【修正6】应用筛选并记录调试信息
df_m = filter_by_date(df_main, date_range)
df_m_original_count = len(df_m)
df_m = apply_brand_filter(df_m, sel_brand)
if sel_cat:
    df_m = df_m[df_m["品类"].isin(sel_cat)]
if sel_center:
    df_m = df_m[df_m["运营中心"].isin(sel_center)]
if sel_area:
    df_m = df_m[df_m["片区"].isin(sel_area)]

df_o = filter_by_date(df_order, date_range)
df_o_original_count = len(df_o)
df_o = apply_brand_filter(df_o, sel_brand)
if sel_cat:
    df_o = df_o[df_o["品类"].isin(sel_cat)]
if sel_center:
    df_o = df_o[df_o["运营中心"].isin(sel_center)]

# 【修正7】增强调试信息 - 东芝金额详细诊断
if st.session_state.debug_mode:
    with st.sidebar:
        st.markdown("---")
        st.subheader("🔎 筛选诊断信息")
        
        # 基础统计
        st.write(f"原始主表行数: {len(df_main)}")
        st.write(f"日期筛选后主表: {df_m_original_count}")
        st.write(f"最终 df_m 行数: {len(df_m)}")
        st.write(f"原始订单行数: {len(df_order)}")
        st.write(f"日期筛选后订单: {df_o_original_count}")
        st.write(f"最终 df_o 行数: {len(df_o)}")
        
        # 东芝详细诊断
        st.markdown("**东芝金额诊断：**")
        
        # 1. 原始数据中的东芝
        raw_toshiba = df_order[df_order["品牌"] == "东芝"]
        st.write(f"原始数据中东芝订单数: {len(raw_toshiba)}")
        if len(raw_toshiba) > 0:
            raw_amount = raw_toshiba["订单金额"].sum()
            st.write(f"原始东芝总金额: {raw_amount:,.2f} 元")
        
        # 2. 日期筛选后的东芝
        date_filtered_toshiba = df_o[df_o["品牌"] == "东芝"]
        st.write(f"日期筛选后东芝订单数: {len(date_filtered_toshiba)}")
        
        # 3. 最终筛选后的东芝
        final_toshiba = df_o[df_o["品牌"] == "东芝"]
        st.write(f"最终筛选后东芝订单数: {len(final_toshiba)}")
        
        if len(final_toshiba) > 0:
            toshiba_amount = final_toshiba["订单金额"].sum()
            st.write(f"最终东芝总金额: {toshiba_amount:,.2f} 元")
            
            # 显示详细订单列表
            st.markdown("**东芝订单明细：**")
            display_cols = ["日期", "品牌", "品牌_原始", "订单金额", "品类", "运营中心", "省市"]
            available_cols = [c for c in display_cols if c in final_toshiba.columns]
            st.dataframe(final_toshiba[available_cols])
            
            # 验证目标金额
            target_amount = 296892
            diff = toshiba_amount - target_amount
            if abs(diff) < 0.01:
                st.success(f"✅ 东芝金额验证通过: {toshiba_amount:,.2f} 元 = 目标 {target_amount:,.2f} 元")
            else:
                st.error(f"❌ 东芝金额不匹配！")
                st.error(f"当前: {toshiba_amount:,.2f} 元")
                st.error(f"目标: {target_amount:,.2f} 元")
                st.error(f"差额: {diff:,.2f} 元 ({diff/target_amount*100:.2f}%)")
                
                # 分析差额原因
                if len(raw_toshiba) != len(final_toshiba):
                    st.warning(f"订单数量变化: {len(raw_toshiba)} → {len(final_toshiba)} (被筛选掉 {len(raw_toshiba) - len(final_toshiba)} 条)")

# 标题
st.markdown('<div class="dashboard-title">🏬 天猫新零售数据看板</div>', unsafe_allow_html=True)
st.markdown(f"<div style='color:#64748b; margin-bottom:1.2rem;'>数据更新至 {max_date}</div>", unsafe_allow_html=True)

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
fig_funnel.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(family="Segoe UI", size=12), margin=dict(l=20, r=20, t=40, b=20))
st.plotly_chart(fig_funnel, use_container_width=True)

# 转化率趋势
st.markdown('<div class="section-header">📈 转化率趋势</div>', unsafe_allow_html=True)

def map_ratio(r):
    if r <= 1.0:
        return r
    else:
        return 1.0 + (r - 1.0) * 0.2

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
    raw_ticks = []
    t = 0.0
    while t <= 1.0 + 1e-9:
        raw_ticks.append(round(t, 6))
        t += 0.1
    t = 1.5
    while t <= 3.6 + 1e-9:
        raw_ticks.append(round(t, 6))
        t += 0.5
    if 3.6 not in raw_ticks:
        raw_ticks.append(3.6)
    raw_ticks = sorted(set(raw_ticks))
    mapped_ticks = [map_ratio(v) for v in raw_ticks]
    tick_labels = [f"{int(v*100)}%" for v in raw_ticks]
    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(x=daily["日期"], y=daily["有效率_mapped"], mode='lines+markers', name='有效率', line=dict(color='#3b82f6', width=2)))
    fig_trend.add_trace(go.Scatter(x=daily["日期"], y=daily["分配率_mapped"], mode='lines+markers', name='分配率', line=dict(color='#10b981', width=2)))
    fig_trend.add_trace(go.Scatter(x=daily["日期"], y=daily["跟进率_mapped"], mode='lines+markers', name='跟进率', line=dict(color='#f59e0b', width=2)))
    fig_trend.add_trace(go.Scatter(x=daily["日期"], y=daily["转化率_mapped"], mode='lines+markers', name='转化率', line=dict(color='#ef4444', width=2)))
    y_max_mapped = map_ratio(3.6)
    fig_trend.update_layout(
        title="转化率趋势（有效率、分配率、跟进率、转化率）<br><sub>注：100%以上区域已压缩</sub>",
        xaxis_title="日期",
        yaxis=dict(title="比率", tickformat='.0%', range=[0, y_max_mapped], tickvals=mapped_ticks, ticktext=tick_labels, tickangle=45),
        legend=dict(x=0.01, y=0.99, bgcolor='rgba(255,255,255,0.8)'),
        hovermode='x unified',
        plot_bgcolor='rgba(0,0,0,0)',
        paper_bgcolor='rgba(0,0,0,0)',
        font=dict(family="Segoe UI", size=12)
    )
    st.plotly_chart(fig_trend, use_container_width=True)
else:
    st.info("无有效日期数据，无法绘制趋势图")

# 销售额分布
st.markdown('<div class="section-header">💰 销售额分布</div>', unsafe_allow_html=True)
if df_o.empty:
    st.warning("当前筛选条件下无订单数据，无法展示销售额分布")
else:
    tab1, tab2, tab3 = st.tabs(["🏷️ 品牌", "📦 品类", "📍 运营中心"])
    with tab1:
        brand_sale = df_o.groupby("品牌")["订单金额"].sum().sort_values(ascending=False).head(10).reset_index()
        brand_sale["万元"] = brand_sale["订单金额"] / 10000
        fig1 = px.bar(brand_sale, x="品牌", y="万元", color="万元", color_continuous_scale="Blues",
                      title="品牌销售额 Top10", text="万元")
        fig1.update_traces(texttemplate='%{text:.1f}', textposition='outside')
        fig1.update_layout(uniformtext_minsize=8, uniformtext_mode='hide', plot_bgcolor='rgba(0,0,0,0)')
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
                      title="运营中心销售额")
        fig3.update_layout(xaxis_tickangle=-45, plot_bgcolor='rgba(0,0,0,0)')
        st.plotly_chart(fig3, use_container_width=True)

# 省份销售额分布
st.markdown('<div class="section-header">🗺️ 省份销售额分布</div>', unsafe_allow_html=True)
st.caption("左侧：各省份销售额排序（横向柱状图）｜右侧：气泡地图（大小代表销售额）")

if df_o.empty:
    st.info("暂无订单数据，无法绘制省份销售额分布")
else:
    if "省市" not in df_o.columns:
        st.error("订单表中缺少'省市'字段，无法按省份统计。")
    else:
        df_o["省份_std"] = df_o["省市"].apply(extract_province_from_shengshi)
        province_sale = df_o.groupby("省份_std")["订单金额"].sum().reset_index()
        province_sale = province_sale[province_sale["省份_std"].notna() & (province_sale["省份_std"] != "")]
        province_sale["万元"] = province_sale["订单金额"] / 10000
        
        if st.session_state.debug_mode:
            with st.expander("🔍 省份提取调试信息"):
                st.write("省市字段样例：", df_o["省市"].dropna().unique()[:20])
                st.write("提取后的标准化省份：", province_sale["省份_std"].tolist())
        
        if province_sale.empty:
            st.warning("未能从'省市'字段中提取到有效省份")
        else:
            province_sale_sorted = province_sale.sort_values("万元", ascending=False)
            col_left, col_right = st.columns([1, 1])
            with col_left:
                st.subheader("📊 各省份销售额排行")
                fig_bar = px.bar(
                    province_sale_sorted,
                    x="万元",
                    y="省份_std",
                    orientation='h',
                    color="万元",
                    color_continuous_scale="Blues",
                    text="万元",
                    title="销售额（万元）",
                    labels={"万元": "销售额(万元)", "省份_std": ""}
                )
                fig_bar.update_traces(texttemplate='%{text:.1f}', textposition='outside')
                fig_bar.update_layout(height=600, yaxis={'categoryorder':'total ascending'}, plot_bgcolor='rgba(0,0,0,0)')
                st.plotly_chart(fig_bar, use_container_width=True)
            with col_right:
                st.subheader("🗺️ 气泡地图（地理分布）")
                province_sale["lon"] = province_sale["省份_std"].apply(
                    lambda p: PROVINCE_CENTER_STD.get(p, [116.4074, 39.9042])[0]
                )
                province_sale["lat"] = province_sale["省份_std"].apply(
                    lambda p: PROVINCE_CENTER_STD.get(p, [116.4074, 39.9042])[1]
                )
                fig_map = go.Figure()
                fig_map.add_trace(go.Scattergeo(
                    lon=province_sale["lon"],
                    lat=province_sale["lat"],
                    mode='markers+text',
                    text=province_sale["省份_std"],
                    textposition="top center",
                    textfont=dict(size=11, color="#1f2937"),
                    marker=dict(
                        size=province_sale["万元"] / province_sale["万元"].max() * 40 + 10,
                        color=province_sale["万元"],
                        colorscale='Blues',
                        showscale=True,
                        colorbar=dict(title="销售额(万元)"),
                        sizemode='area'
                    ),
                    hoverinfo='text',
                    hovertext=province_sale.apply(lambda r: f"{r['省份_std']}<br>销售额: {r['万元']:.2f}万元", axis=1)
                ))
                fig_map.update_layout(
                    title="气泡大小代表销售额",
                    geo=dict(scope='asia', center=dict(lat=35, lon=105), projection_scale=1.2, showland=True, landcolor='rgb(243,243,243)'),
                    height=600, margin={"r":0,"t":40,"l":0,"b":0}, paper_bgcolor='rgba(0,0,0,0)'
                )
                st.plotly_chart(fig_map, use_container_width=True)

# 明细核对
with st.expander("📄 订单明细（核对总金额）"):
    if not df_o.empty:
        cols_to_show = [c for c in ["日期", "品牌", "品牌_原始", "品类", "运营中心", "市区", "省市", "订单金额"] if c in df_o.columns]
        st.dataframe(df_o[cols_to_show], use_container_width=True)
        st.success(f"✅ 订单总金额：{total_amount:,.2f} 元  =  {total_wan:.2f} 万元")
        
        # 【新增】各品牌金额汇总核对
        st.markdown("**各品牌金额核对：**")
        brand_check = df_o.groupby("品牌")["订单金额"].agg(['count', 'sum']).round(2)
        brand_check.columns = ['订单数', '总金额(元)']
        brand_check['总金额(万元)'] = (brand_check['总金额(元)'] / 10000).round(4)
        st.dataframe(brand_check, use_container_width=True)
        
        # 特别标注东芝
        if "东芝" in brand_check.index:
            toshiba_row = brand_check.loc["东芝"]
            st.info(f"💡 东芝: {toshiba_row['订单数']} 单, {toshiba_row['总金额(元)']:,.2f} 元 ({toshiba_row['总金额(万元)']:.4f} 万元)")
    else:
        st.info("无订单明细")
'''

# 保存到文件
output_path = '/mnt/kimi/output/tmall_dashboard_fixed.py'
with open(output_path, 'w', encoding='utf-8') as f:
    f.write(complete_code)

print(f"✅ 完整代码已生成并保存到: {output_path}")
print(f"\n文件大小: {len(complete_code)} 字符")
print(f"\n主要修正点:")
print("1. ✅ 增强品牌标准化函数 - 支持更多东芝变体（東芝、toshiba等）")
print("2. ✅ 保留原始品牌字段 - 添加'品牌_原始'列用于调试对比")
print("3. ✅ 增强调试面板 - 显示品牌标准化前后的详细对比")
print("4. ✅ 东芝专项诊断 - 在调试模式下显示东芝金额的详细计算过程")
print("5. ✅ 订单金额处理优化 - 保留原始值，显示NaN值统计")
print("6. ✅ 筛选逻辑增强 - 记录筛选前后的数据量变化")
print("7. ✅ 订单明细增强 - 显示各品牌金额汇总核对表")
