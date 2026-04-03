import streamlit as st
import pandas as pd
import sqlite3
import zipfile
import tempfile
import os

st.set_page_config(page_title="订单金额看板", layout="wide")
st.title("📊 每日订单金额趋势")

@st.cache_data(ttl=86400)
def load_data():
    # 1. 读取 data.zip 中的数据库文件
    with zipfile.ZipFile('data.zip', 'r') as z:
        db_files = [f for f in z.namelist() if f.endswith('.db')]
        if not db_files:
            st.error("data.zip 中没有找到 .db 文件")
            st.stop()
        db_filename = db_files[0]
        with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
            with z.open(db_filename) as source:
                tmp.write(source.read())
            tmp_path = tmp.name

    # 2. 连接临时数据库并查询
    conn = sqlite3.connect(tmp_path)
    query = """
        SELECT 
            o."日期",
            o."订单金额",
            k."意向品牌" AS "品牌",
            k."品类",
            k."运营中心",
            k."获取时间"
        FROM "订单表" o
        INNER JOIN "客资明细表" k 
            ON o."日期" = k."获取时间"
            AND o."品牌" = k."意向品牌"
            AND o."品类" = k."品类"
            AND o."运中" = k."运营中心"
        WHERE o."日期" IS NOT NULL
        ORDER BY o."日期" DESC
        LIMIT 5000
    """
    try:
        df = pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"SQL查询失败: {e}")
        st.stop()
    conn.close()
    os.unlink(tmp_path)
    return df

df = load_data()
st.success(f"✅ 成功加载 {len(df)} 条数据")

with st.expander("📄 查看关联后的数据"):
    st.dataframe(df)

st.subheader("📈 每日订单金额趋势")
date_col = '日期'
value_col = '订单金额'

df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
df_valid = df.dropna(subset=[date_col, value_col])

if len(df_valid) > 0:
    daily = df_valid.groupby(df_valid[date_col].dt.date)[value_col].sum()
    st.line_chart(daily)
else:
    st.warning("没有有效的订单日期或金额数据。")

st.subheader("📊 各品牌订单金额（Top 10）")
if '品牌' in df.columns and value_col in df.columns:
    grouped = df.groupby('品牌')[value_col].sum().sort_values(ascending=False).head(10)
    st.bar_chart(grouped)
else:
    st.info("品牌列不存在或金额列缺失。")

st.subheader("📊 各品类订单金额分布")
if '品类' in df.columns and value_col in df.columns:
    grouped2 = df.groupby('品类')[value_col].sum().sort_values(ascending=False).head(10)
    st.bar_chart(grouped2)
