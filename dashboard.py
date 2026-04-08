import streamlit as st
import pandas as pd
import sqlite3
import zipfile
import tempfile
import os
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
import requests

st.set_page_config(layout="wide", page_title="天猫新零售数据看板", page_icon="📊")

# ----------------------------- 辅助函数 ---------------------------------
def extract_city_name(location):
    """（保留原函数，但省份分布不再使用）"""
    if not location or pd.isna(location):
        return None
    loc = str(location).strip()
    for suffix in ["市", "区", "县"]:
        if loc.endswith(suffix):
            loc = loc[:-len(suffix)]
            break
    # 直辖市辖区处理略（可保留原代码）
    return loc

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

# ----------------------------- 数据加载 ---------------------------------
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

    # 统一字段名
    for df in [df_main, df_order]:
        df["品牌"] = df.get("品牌", df.get("意向品牌", "未知")).fillna("未知")
        df["品类"] = df.get("品类", "未知").fillna("未知")
        df["运营中心"] = df.get("运营中心", df.get("运中", "未知")).fillna("未知")
        df["片区"] = df.get("片区", "未知").fillna("未知")
    if "外呼状态" not in df_main.columns:
        df_main["外呼状态"] = ""
    if "最新跟进状态" not in df_main.columns:
        df_main["最新跟进状态"] = ""
    if "市区" not in df_order.columns:
        df_order["市区"] = ""
    if "省市" not in df_order.columns:
        st.warning("订单表中缺少'省市'字段，无法绘制省份地图")
        df_order["省市"] = ""

    return df_main, df_order

# ----------------------------- 省份提取函数（基于"省市"字段） -----------------------------
def extract_province_from_shengshi(shengshi):
    if pd.isna(shengshi) or not shengshi:
        return None
    s = str(shengshi).strip()
    # 直辖市标准化
    if s in ['北京', '北京市', '北京直辖市']:
        return '北京市'
    if s in ['上海', '上海市', '上海直辖市']:
        return '上海市'
    if s in ['天津', '天津市', '天津直辖市']:
        return '天津市'
    if s in ['重庆', '重庆市', '重庆直辖市']:
        return '重庆市'
    # 普通省份带"省"
    if s.endswith('省'):
        return s
    # 常见缺省字的情况
    common = ['江苏', '浙江', '广东', '山东', '河南', '四川', '湖北', '湖南', '河北', '福建', '安徽', '辽宁', '江西', '陕西', '山西', '云南', '贵州', '甘肃', '青海', '吉林', '黑龙江', '海南', '台湾']
    if s in common:
        return s + '省'
    # 自治区
    if s in ['广西', '广西壮族自治区']:
        return '广西壮族自治区'
    if s in ['内蒙古', '内蒙古自治区']:
        return '内蒙古自治区'
    if s in ['宁夏', '宁夏回族自治区']:
        return '宁夏回族自治区'
    if s in ['新疆', '新疆维吾尔自治区']:
        return '新疆维吾尔自治区'
    if s in ['西藏', '西藏自治区']:
        return '西藏自治区'
    return s  # 其他原样返回

# 加载中国GeoJSON
@st.cache_data(show_spinner="加载中国地图数据...")
def get_china_geojson():
    url = "https://raw.githubusercontent.com/geoi18/China-GeoJSON/master/Province.geojson"
    try:
        response = requests.get(url, timeout=8)
        if response.status_code == 200:
            return response.json()
    except:
        pass
    return None

# ----------------------------- 主程序 ---------------------------------
df_main, df_order = load_data()
if df_main.empty:
    st.error("客资明细表为空，请检查数据源")
    st.stop()

if "debug_mode" not in st.session_state:
    st.session_state.debug_mode = False
with st.sidebar:
    st.session_state.debug_mode = st.checkbox("🔧 调试模式", value=False)

actual_brands = sorted([b for b in df_main["品牌"].dropna().unique() if b and b != "未知"])
actual_cats = sorted([c for c in df_main["品类"].dropna().unique() if c and c != "未知"])
actual_centers = sorted([c for c in df_main["运营中心"].dropna().unique() if c and c != "未知"])
actual_areas = sorted([a for a in df_main["片区"].dropna().unique() if a and a != "未知"])
brand_options = actual_brands + ["洗衣机汇总", "美的厨热", "美的冰箱", "美的空调"]

st.sidebar.header("🔍 筛选条件")
if not df_main["日期"].isna().all():
    min_date = df_main["日期"].min().date()
    max_date = df_main["日期"].max().date()
else:
    min_date = datetime.today().date()
    max_date = datetime.today().date()
date_range = st.sidebar.date_input("日期范围", [min_date, max_date])

col1_s, col2_s = st.sidebar.columns(2)
with col1_s:
    sel_brand = st.multiselect("品牌", brand_options, default=actual_brands)
    sel_cat = st.multiselect("品类", actual_cats, default=actual_cats)
with col2_s:
    sel_area = st.multiselect("片区", actual_areas, default=actual_areas)
    sel_center = st.multiselect("运营中心", actual_centers, default=actual_centers)

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

if st.session_state.debug_mode:
    with st.sidebar:
        st.markdown("---")
        st.subheader("🔎 诊断信息")
        st.write(f"原始主表行数: {len(df_main)}")
        st.write(f"日期筛选后行数: {len(filter_by_date(df_main, date_range))}")
        st.write(f"最终 df_m 行数: {len(df_m)}")
        if len(df_m) == 0:
            st.error("❌ 无数据，请检查筛选条件")

# 指标卡片
st.title("🏬 天猫新零售数据看板")
c1, c2, c3, c4 = st.columns(4)
total_leads = len(df_m)
valid_mask = df_m["外呼状态"].isin(["高意向", "低意向", "无需外呼"])
valid_leads = valid_mask.sum()
order_count = len(df_o)
total_amount = df_o["订单金额"].sum() if not df_o.empty else 0.0
total_wan = total_amount / 10000

c1.metric("总客资", f"{total_leads:,}")
c2.metric("有效客资", f"{valid_leads:,}")
c3.metric("成交单量", f"{order_count:,}")
c4.metric("总金额（万元）", f"{total_wan:.2f}")

# 转化漏斗
st.header("📉 转化漏斗")
if "最新跟进状态" in df_m.columns and not df_m.empty:
    assigned = df_m[valid_mask & (df_m["最新跟进状态"] != "未分配")].shape[0]
    followed = df_m[valid_mask & (~df_m["最新跟进状态"].isin(["未分配", "待查看", "待联系"]))].shape[0]
else:
    assigned = 0
    followed = 0
funnel_labels = ["总客资", "有效客资", "已分配", "已跟进", "成交"]
funnel_values = [total_leads, valid_leads, assigned, followed, order_count]
fig_funnel = go.Figure(go.Funnel(y=funnel_labels, x=funnel_values))
st.plotly_chart(fig_funnel, use_container_width=True)

# 转化率趋势
st.header("📈 转化率趋势")
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
    fig_trend.add_trace(go.Scatter(x=daily["日期"], y=daily["有效率_mapped"], mode='lines+markers', name='有效率'))
    fig_trend.add_trace(go.Scatter(x=daily["日期"], y=daily["分配率_mapped"], mode='lines+markers', name='分配率'))
    fig_trend.add_trace(go.Scatter(x=daily["日期"], y=daily["跟进率_mapped"], mode='lines+markers', name='跟进率'))
    fig_trend.add_trace(go.Scatter(x=daily["日期"], y=daily["转化率_mapped"], mode='lines+markers', name='转化率'))
    y_max_mapped = map_ratio(3.6)
    fig_trend.update_layout(
        title="转化率趋势（有效率、分配率、跟进率、转化率）<br><sub>注：100%以上区域已压缩</sub>",
        xaxis_title="日期",
        yaxis=dict(title="比率", tickformat='.0%', range=[0, y_max_mapped], tickvals=mapped_ticks, ticktext=tick_labels, tickangle=45),
        legend=dict(x=0.01, y=0.99),
        hovermode='x unified'
    )
    st.plotly_chart(fig_trend, use_container_width=True)
else:
    st.info("无有效日期数据，无法绘制趋势图")

# 销售额分布（品牌、品类、运营中心）
st.header("💰 销售额分布")
if df_o.empty:
    st.warning("当前筛选条件下无订单数据，无法展示销售额分布")
else:
    tab1, tab2, tab3 = st.tabs(["品牌", "品类", "运营中心"])
    with tab1:
        brand_sale = df_o.groupby("品牌")["订单金额"].sum().sort_values(ascending=False).head(10).reset_index()
        brand_sale["万元"] = brand_sale["订单金额"] / 10000
        fig1 = px.bar(brand_sale, x="品牌", y="万元", color="万元", title="品牌销售额 Top10")
        st.plotly_chart(fig1, use_container_width=True)
    with tab2:
        cat_sale = df_o.groupby("品类")["订单金额"].sum().reset_index()
        cat_sale["万元"] = cat_sale["订单金额"] / 10000
        fig2 = px.pie(cat_sale, names="品类", values="万元", title="品类销售额占比")
        st.plotly_chart(fig2, use_container_width=True)
    with tab3:
        center_sale = df_o.groupby("运营中心")["订单金额"].sum().reset_index()
        center_sale["万元"] = center_sale["订单金额"] / 10000
        fig3 = px.bar(center_sale, x="运营中心", y="万元", color="万元", title="运营中心销售额")
        st.plotly_chart(fig3, use_container_width=True)

# ======================= 修改后的省份销售额分布（直接使用"省市"字段，热力图优先） =======================
st.header("🗺️ 省份销售额分布")
st.caption("省份销售额热力图（填充地图）")

if df_o.empty:
    st.info("暂无订单数据，无法绘制省份销售额分布")
else:
    # 检查是否有"省市"字段
    if "省市" not in df_o.columns:
        st.error("订单表中缺少'省市'字段，无法按省份统计。请检查数据源。")
    else:
        # 提取省份
        df_o["省份_std"] = df_o["省市"].apply(extract_province_from_shengshi)
        province_sale = df_o.groupby("省份_std")["订单金额"].sum().reset_index()
        province_sale = province_sale[province_sale["省份_std"].notna() & (province_sale["省份_std"] != "")]
        province_sale["万元"] = province_sale["订单金额"] / 10000
        
        if st.session_state.debug_mode:
            with st.expander("🔍 省份提取调试信息"):
                st.write("省市字段样例（去重前20）：", df_o["省市"].dropna().unique()[:20])
                st.write("提取后的省份样例：", province_sale["省份_std"].tolist())
        
        if province_sale.empty:
            st.warning("未能从'省市'字段中提取到有效省份，请检查数据格式。")
        else:
            # 尝试加载 GeoJSON
            china_geojson = get_china_geojson()
            if china_geojson:
                try:
                    # 注意：GeoJSON 中省份的字段名通常是 "name"
                    fig_map = px.choropleth(
                        province_sale,
                        geojson=china_geojson,
                        locations="省份_std",
                        featureidkey="properties.name",  # 根据实际 GeoJSON 调整
                        color="万元",
                        color_continuous_scale="Blues",
                        range_color=(0, province_sale["万元"].max()),
                        hover_name="省份_std",
                        hover_data={"万元": ":,.2f"},
                        title="全国省份销售额热力图（万元）"
                    )
                    fig_map.update_geos(fitbounds="locations", visible=False)
                    fig_map.update_layout(margin={"r":0,"t":50,"l":0,"b":0}, height=700)
                    st.plotly_chart(fig_map, use_container_width=True)
                except Exception as e:
                    st.warning(f"热力图渲染失败: {e}，将使用气泡图代替")
                    china_geojson = None  # 降级
            if not china_geojson:
                # 降级方案：气泡地图
                # 为省份添加中心坐标
                province_coords = []
                for prov in province_sale["省份_std"]:
                    # 映射省份简称到中心坐标（需要简化名称）
                    simple_name = prov.replace('省', '').replace('市', '').replace('自治区', '')
                    if simple_name in ['北京', '上海', '天津', '重庆']:
                        simple_name = simple_name + '市'
                    elif simple_name == '广西':
                        simple_name = '广西'
                    elif simple_name == '内蒙古':
                        simple_name = '内蒙古'
                    elif simple_name == '宁夏':
                        simple_name = '宁夏'
                    elif simple_name == '新疆':
                        simple_name = '新疆'
                    elif simple_name == '西藏':
                        simple_name = '西藏'
                    coords = PROVINCE_CENTER.get(simple_name, [116.4074, 39.9042])
                    province_coords.append(coords)
                province_sale["lon"] = [c[0] for c in province_coords]
                province_sale["lat"] = [c[1] for c in province_coords]
                fig_map = px.scatter_geo(
                    province_sale,
                    lon="lon", lat="lat",
                    size="万元", color="万元",
                    hover_name="省份_std",
                    projection="natural earth",
                    title="全国省份销售额分布（气泡图，因热力图数据不可用）",
                    color_continuous_scale="Blues",
                    size_max=60
                )
                fig_map.update_layout(
                    geo=dict(scope='asia', center=dict(lat=35, lon=105), projection_scale=1.2),
                    margin={"r":0,"t":50,"l":0,"b":0},
                    height=700
                )
                st.plotly_chart(fig_map, use_container_width=True)

# 明细核对
with st.expander("📄 订单明细（核对总金额）"):
    if not df_o.empty:
        st.dataframe(df_o[["日期", "品牌", "品类", "运营中心", "市区", "省市", "订单金额"]], use_container_width=True)
        st.success(f"✅ 订单总金额：{total_amount:,.2f} 元  =  {total_wan:.2f} 万元")
    else:
        st.info("无订单明细")
