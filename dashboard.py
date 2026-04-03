import streamlit as st
import pandas as pd
import sqlite3
import zipfile
import io

st.set_page_config(page_title="订单金额看板", layout="wide")
st.title("📊 每日订单金额趋势")

@st.cache_data(ttl=86400)
def load_data():
    # 从 zip 文件中读取 SQLite 数据库
    with zipfile.ZipFile('data.db.zip', 'r') as z:
        with z.open('data.db') as f:
            data = f.read()
    # 创建内存中的数据库连接
    conn = sqlite3.connect(io.BytesIO(data))
    query = """
        SELECT 
            o.`日期`,
            o.`订单金额`,
            k.`意向品牌` AS `品牌`,
            k.`品类`,
            k.`运营中心`,
            k.`获取时间`
        FROM `订单表` o
        INNER JOIN `客资明细表` k 
            ON o.`日期` = k.`获取时间`
            AND o.`品牌` = k.`意向品牌`
            AND o.`品类` = k.`品类`
            AND o.`运中` = k.`运营中心`
        WHERE o.`日期` IS NOT NULL
        ORDER BY o.`日期` DESC
        LIMIT 5000
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

df = load_data()
st.success(f"✅ 成功加载 {len(df)} 条数据")
# 后续图表代码不变（省略，请保留之前的图表部分）