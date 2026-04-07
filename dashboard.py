import streamlit as st
import pandas as pd
import sqlite3
import zipfile
import tempfile
import os
from datetime import datetime
import plotly.express as px
import plotly.graph_objects as go
import json
import requests

st.set_page_config(layout="wide", page_title="天猫新零售数据看板", page_icon="📊")

# ----------------------------- 全国城市坐标（备用） -----------------------------
CITY_COORDS = {
    '北京': [116.4074, 39.9042], '上海': [121.4737, 31.2304], '天津': [117.1902, 39.1256], '重庆': [106.5044, 29.5582],
    '石家庄': [114.5149, 38.0428], '唐山': [118.1827, 39.6307], '秦皇岛': [119.5219, 39.9307], '邯郸': [114.5261, 36.6052],
    '邢台': [114.5093, 37.0671], '保定': [115.4624, 38.8735], '张家口': [114.8722, 40.8286], '承德': [117.9327, 40.9501],
    '沧州': [116.8328, 38.3141], '廊坊': [116.6821, 39.1327], '衡水': [115.7230, 37.7401],
    '太原': [112.5624, 37.8735], '大同': [113.2930, 40.0761], '阳泉': [113.5699, 37.8671], '长治': [113.1269, 36.1919],
    '晋城': [112.8527, 35.4951], '朔州': [112.4305, 39.3281], '晋中': [112.7370, 37.7501], '运城': [110.9901, 35.0271],
    '忻州': [112.7317, 38.4151], '临汾': [111.5172, 36.0851], '吕梁': [111.1324, 37.5191],
    '呼和浩特': [111.7510, 40.8415], '包头': [109.8386, 40.6591], '乌海': [106.8206, 39.6711], '赤峰': [118.9401, 42.2591],
    '通辽': [122.2658, 43.6491], '鄂尔多斯': [109.7814, 39.6111], '呼伦贝尔': [119.7541, 49.2121], '巴彦淖尔': [107.4026, 40.7441],
    '乌兰察布': [113.1258, 41.0601], '兴安盟': [122.5091, 46.0801], '锡林郭勒': [116.1001, 43.9501], '阿拉善': [105.7101, 38.8501],
    '沈阳': [123.4315, 41.8057], '大连': [121.6207, 38.9157], '鞍山': [122.9967, 41.1067], '抚顺': [123.9527, 41.8767],
    '本溪': [123.7677, 41.2967], '丹东': [124.3797, 40.1337], '锦州': [121.1317, 41.1007], '营口': [122.2317, 40.6667],
    '阜新': [121.6497, 42.0207], '辽阳': [123.1727, 41.2767], '盘锦': [122.0697, 41.1197], '铁岭': [123.8497, 42.2907],
    '朝阳': [120.4517, 41.5707], '葫芦岛': [120.8417, 40.7107],
    '长春': [125.3235, 43.8171], '吉林': [126.5515, 43.8371], '四平': [124.3605, 43.1601], '辽源': [125.1305, 42.8901],
    '通化': [125.9305, 41.7301], '白山': [126.4105, 41.9401], '松原': [124.8205, 45.1201], '白城': [122.8305, 45.6201],
    '延边': [129.5005, 42.9001],
    '哈尔滨': [126.5364, 45.8022], '齐齐哈尔': [123.9644, 47.3422], '鸡西': [130.9944, 45.2922], '鹤岗': [130.2844, 47.3322],
    '双鸭山': [131.1744, 46.6422], '大庆': [125.1344, 46.5822], '伊春': [128.9344, 47.7222], '佳木斯': [130.3744, 46.8122],
    '七台河': [130.9844, 45.8022], '牡丹江': [129.6344, 44.5822], '黑河': [127.5344, 50.2322], '绥化': [126.9844, 46.6322],
    '南京': [118.7674, 32.0415], '无锡': [120.3136, 31.4908], '徐州': [117.1387, 34.2633], '常州': [119.9740, 31.8107],
    '苏州': [120.5853, 31.2989], '南通': [120.8646, 32.0167], '连云港': [119.1629, 34.5927], '淮安': [119.0212, 33.6268],
    '盐城': [120.1399, 33.3776], '扬州': [119.4256, 32.3937], '镇江': [119.4529, 32.2044], '泰州': [119.9078, 32.4588],
    '杭州': [120.1551, 30.2741], '宁波': [121.5438, 29.8683], '温州': [120.6994, 27.9949], '嘉兴': [120.7508, 30.7536],
    '湖州': [120.0997, 30.8703], '绍兴': [120.5818, 30.0082], '金华': [119.6407, 29.0895], '衢州': [118.8803, 28.9703],
    '舟山': [122.2072, 29.9856], '台州': [121.4208, 28.6563], '丽水': [119.9152, 28.4527],
    '合肥': [117.2272, 31.8206], '芜湖': [118.3830, 31.3533], '蚌埠': [117.3583, 32.9183], '淮南': [116.9985, 32.6476],
    '福州': [119.2965, 26.0745], '厦门': [118.0895, 24.4798], '莆田': [119.0025, 25.4405], '三明': [117.6405, 26.2505],
    '南昌': [115.8582, 28.6820], '济南': [117.0009, 36.6758], '青岛': [120.3826, 36.0671], '郑州': [113.6254, 34.7466],
    '武汉': [114.3055, 30.5931], '长沙': [112.9388, 28.2282], '广州': [113.2644, 23.1291], '深圳': [114.0579, 22.5431],
    '南宁': [108.3661, 22.8176], '海口': [110.1999, 20.0440], '成都': [104.0668, 30.5728], '贵阳': [106.6302, 26.6477],
    '昆明': [102.8329, 24.8801], '拉萨': [91.1409, 29.6565], '西安': [108.9402, 34.3416], '兰州': [103.8343, 36.0611],
    '西宁': [101.7782, 36.6232], '银川': [106.2309, 38.4872], '乌鲁木齐': [87.6168, 43.8256]
}
DEFAULT_COORD = [116.4074, 39.9042]

# ----------------------------- 省份映射表（用于热力图） -----------------------------
CITY_TO_PROVINCE = {
    '北京': '北京', '上海': '上海', '天津': '天津', '重庆': '重庆',
    '石家庄': '河北', '唐山': '河北', '秦皇岛': '河北', '邯郸': '河北', '邢台': '河北', '保定': '河北', '张家口': '河北', '承德': '河北', '沧州': '河北', '廊坊': '河北', '衡水': '河北',
    '太原': '山西', '大同': '山西', '阳泉': '山西', '长治': '山西', '晋城': '山西', '朔州': '山西', '晋中': '山西', '运城': '山西', '忻州': '山西', '临汾': '山西', '吕梁': '山西',
    '呼和浩特': '内蒙古', '包头': '内蒙古', '乌海': '内蒙古', '赤峰': '内蒙古', '通辽': '内蒙古', '鄂尔多斯': '内蒙古', '呼伦贝尔': '内蒙古', '巴彦淖尔': '内蒙古', '乌兰察布': '内蒙古', '兴安盟': '内蒙古', '锡林郭勒': '内蒙古', '阿拉善': '内蒙古',
    '沈阳': '辽宁', '大连': '辽宁', '鞍山': '辽宁', '抚顺': '辽宁', '本溪': '辽宁', '丹东': '辽宁', '锦州': '辽宁', '营口': '辽宁', '阜新': '辽宁', '辽阳': '辽宁', '盘锦': '辽宁', '铁岭': '辽宁', '朝阳': '辽宁', '葫芦岛': '辽宁',
    '长春': '吉林', '吉林': '吉林', '四平': '吉林', '辽源': '吉林', '通化': '吉林', '白山': '吉林', '松原': '吉林', '白城': '吉林', '延边': '吉林',
    '哈尔滨': '黑龙江', '齐齐哈尔': '黑龙江', '鸡西': '黑龙江', '鹤岗': '黑龙江', '双鸭山': '黑龙江', '大庆': '黑龙江', '伊春': '黑龙江', '佳木斯': '黑龙江', '七台河': '黑龙江', '牡丹江': '黑龙江', '黑河': '黑龙江', '绥化': '黑龙江',
    '南京': '江苏', '无锡': '江苏', '徐州': '江苏', '常州': '江苏', '苏州': '江苏', '南通': '江苏', '连云港': '江苏', '淮安': '江苏', '盐城': '江苏', '扬州': '江苏', '镇江': '江苏', '泰州': '江苏',
    '杭州': '浙江', '宁波': '浙江', '温州': '浙江', '嘉兴': '浙江', '湖州': '浙江', '绍兴': '浙江', '金华': '浙江', '衢州': '浙江', '舟山': '浙江', '台州': '浙江', '丽水': '浙江',
    '合肥': '安徽', '芜湖': '安徽', '蚌埠': '安徽', '淮南': '安徽', '马鞍山': '安徽', '淮北': '安徽', '铜陵': '安徽', '安庆': '安徽', '黄山': '安徽', '滁州': '安徽', '阜阳': '安徽', '宿州': '安徽', '六安': '安徽', '亳州': '安徽', '池州': '安徽', '宣城': '安徽',
    '福州': '福建', '厦门': '福建', '莆田': '福建', '三明': '福建', '泉州': '福建', '漳州': '福建', '南平': '福建', '龙岩': '福建', '宁德': '福建',
    '南昌': '江西', '景德镇': '江西', '萍乡': '江西', '九江': '江西', '新余': '江西', '鹰潭': '江西', '赣州': '江西', '吉安': '江西', '宜春': '江西', '抚州': '江西', '上饶': '江西',
    '济南': '山东', '青岛': '山东', '淄博': '山东', '枣庄': '山东', '东营': '山东', '烟台': '山东', '潍坊': '山东', '济宁': '山东', '泰安': '山东', '威海': '山东', '日照': '山东', '临沂': '山东', '德州': '山东', '聊城': '山东', '滨州': '山东', '菏泽': '山东',
    '郑州': '河南', '开封': '河南', '洛阳': '河南', '平顶山': '河南', '安阳': '河南', '鹤壁': '河南', '新乡': '河南', '焦作': '河南', '濮阳': '河南', '许昌': '河南', '漯河': '河南', '三门峡': '河南', '南阳': '河南', '商丘': '河南', '信阳': '河南', '周口': '河南', '驻马店': '河南',
    '武汉': '湖北', '黄石': '湖北', '十堰': '湖北', '宜昌': '湖北', '襄阳': '湖北', '鄂州': '湖北', '荆门': '湖北', '孝感': '湖北', '荆州': '湖北', '黄冈': '湖北', '咸宁': '湖北', '随州': '湖北', '恩施': '湖北',
    '长沙': '湖南', '株洲': '湖南', '湘潭': '湖南', '衡阳': '湖南', '邵阳': '湖南', '岳阳': '湖南', '常德': '湖南', '张家界': '湖南', '益阳': '湖南', '郴州': '湖南', '永州': '湖南', '怀化': '湖南', '娄底': '湖南', '湘西': '湖南',
    '广州': '广东', '深圳': '广东', '珠海': '广东', '汕头': '广东', '佛山': '广东', '江门': '广东', '湛江': '广东', '茂名': '广东', '肇庆': '广东', '惠州': '广东', '梅州': '广东', '汕尾': '广东', '河源': '广东', '阳江': '广东', '清远': '广东', '东莞': '广东', '中山': '广东', '潮州': '广东', '揭阳': '广东', '云浮': '广东',
    '南宁': '广西', '柳州': '广西', '桂林': '广西', '梧州': '广西', '北海': '广西', '防城港': '广西', '钦州': '广西', '贵港': '广西', '玉林': '广西', '百色': '广西', '贺州': '广西', '河池': '广西', '来宾': '广西', '崇左': '广西',
    '海口': '海南', '三亚': '海南', '三沙': '海南', '儋州': '海南',
    '成都': '四川', '自贡': '四川', '攀枝花': '四川', '泸州': '四川', '德阳': '四川', '绵阳': '四川', '广元': '四川', '遂宁': '四川', '内江': '四川', '乐山': '四川', '南充': '四川', '眉山': '四川', '宜宾': '四川', '广安': '四川', '达州': '四川', '雅安': '四川', '巴中': '四川', '资阳': '四川', '阿坝': '四川', '甘孜': '四川', '凉山': '四川',
    '贵阳': '贵州', '六盘水': '贵州', '遵义': '贵州', '安顺': '贵州', '毕节': '贵州', '铜仁': '贵州', '黔西南': '贵州', '黔东南': '贵州', '黔南': '贵州',
    '昆明': '云南', '曲靖': '云南', '玉溪': '云南', '保山': '云南', '昭通': '云南', '丽江': '云南', '普洱': '云南', '临沧': '云南', '楚雄': '云南', '红河': '云南', '文山': '云南', '西双版纳': '云南', '大理': '云南', '德宏': '云南', '怒江': '云南', '迪庆': '云南',
    '拉萨': '西藏', '日喀则': '西藏', '昌都': '西藏', '林芝': '西藏', '山南': '西藏', '那曲': '西藏', '阿里': '西藏',
    '西安': '陕西', '铜川': '陕西', '宝鸡': '陕西', '咸阳': '陕西', '渭南': '陕西', '延安': '陕西', '汉中': '陕西', '榆林': '陕西', '安康': '陕西', '商洛': '陕西',
    '兰州': '甘肃', '嘉峪关': '甘肃', '金昌': '甘肃', '白银': '甘肃', '天水': '甘肃', '武威': '甘肃', '张掖': '甘肃', '平凉': '甘肃', '酒泉': '甘肃', '庆阳': '甘肃', '定西': '甘肃', '陇南': '甘肃', '临夏': '甘肃', '甘南': '甘肃',
    '西宁': '青海', '海东': '青海', '海北': '青海', '黄南': '青海', '海南州': '青海', '果洛': '青海', '玉树': '青海', '海西': '青海',
    '银川': '宁夏', '石嘴山': '宁夏', '吴忠': '宁夏', '固原': '宁夏', '中卫': '宁夏',
    '乌鲁木齐': '新疆', '克拉玛依': '新疆', '吐鲁番': '新疆', '哈密': '新疆', '昌吉': '新疆', '博尔塔拉': '新疆', '巴音郭楞': '新疆', '阿克苏': '新疆', '克孜勒苏': '新疆', '喀什': '新疆', '和田': '新疆', '伊犁': '新疆', '塔城': '新疆', '阿勒泰': '新疆',
}

def extract_city_name(location):
    """从订单表的市区字段提取城市名"""
    if not location or pd.isna(location):
        return None
    loc = str(location).strip()
    for suffix in ["市", "区", "县"]:
        if loc.endswith(suffix):
            loc = loc[:-len(suffix)]
            break
    beijing_districts = ['东城', '西城', '朝阳', '海淀', '丰台', '石景山', '门头沟', '房山', '通州', '顺义', '昌平', '大兴', '怀柔', '平谷', '密云', '延庆']
    if loc in beijing_districts:
        return '北京'
    shanghai_districts = ['黄浦', '徐汇', '长宁', '静安', '普陀', '虹口', '杨浦', '闵行', '宝山', '嘉定', '浦东', '金山', '松江', '青浦', '奉贤', '崇明']
    if loc in shanghai_districts:
        return '上海'
    tianjin_districts = ['和平', '河东', '河西', '南开', '河北', '红桥', '东丽', '西青', '津南', '北辰', '武清', '宝坻', '滨海', '宁河', '静海', '蓟州']
    if loc in tianjin_districts:
        return '天津'
    chongqing_districts = ['万州', '涪陵', '渝中', '大渡口', '江北', '沙坪坝', '九龙坡', '南岸', '北碚', '綦江', '大足', '渝北', '巴南', '黔江', '长寿', '江津', '合川', '永川', '南川', '璧山', '铜梁', '潼南', '荣昌', '开州', '梁平', '武隆']
    if loc in chongqing_districts:
        return '重庆'
    return loc

# ----------------------------- 品牌筛选逻辑 -----------------------------
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

    return df_main, df_order

# ----------------------------- 加载标准中国地图GeoJSON（双源降级） -----------------------------
@st.cache_data(show_spinner="加载中国地图数据...")
def get_china_geojson():
    urls = [
        "https://geo.datav.aliyun.com/areas_v3/bound/100000_full.json",
        "https://cdn.jsdelivr.net/npm/china-geojson@1.0.0/100000_full.json"
    ]
    for url in urls:
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                return response.json()
        except:
            continue
    return None

# ----------------------------- 主程序 -----------------------------
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
            st.error("❌ 无数据，请检查筛选条件与数据中的实际值是否匹配")
            st.write("品牌唯一值:", df_main["brand"].unique() if "brand" in df_main else df_main["品牌"].unique())
            st.write("品类唯一值:", df_main["品类"].unique())

# ----------------------------- 指标卡片 -----------------------------
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

# ----------------------------- 转化漏斗 -----------------------------
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

# ----------------------------- 转化率趋势 -----------------------------
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
        yaxis=dict(
            title="比率",
            tickformat='.0%',
            range=[0, y_max_mapped],
            tickvals=mapped_ticks,
            ticktext=tick_labels,
            tickangle=45,
            tickfont=dict(size=10)
        ),
        legend=dict(x=0.01, y=0.99),
        hovermode='x unified'
    )
    st.plotly_chart(fig_trend, use_container_width=True)
else:
    st.info("无有效日期数据，无法绘制趋势图")

# ----------------------------- 销售额分布 -----------------------------
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

# ----------------------------- 省份销售额分布（双保险：优先填充地图，降级气泡图） -----------------------------
st.header("🗺️ 省份销售额分布")
st.caption("省份销售额分布（颜色/气泡大小代表销售额，单位：万元）")

if not df_o.empty and "市区" in df_o.columns:
    # 提取省份
    df_o["城市"] = df_o["市区"].apply(extract_city_name)
    df_o["省份"] = df_o["城市"].map(CITY_TO_PROVINCE)
    province_sale = df_o.groupby("省份")["订单金额"].sum().reset_index()
    province_sale = province_sale[province_sale["省份"].notna() & (province_sale["省份"] != "")]
    province_sale["万元"] = province_sale["订单金额"] / 10000
    
    if not province_sale.empty:
        # 省份中心坐标（用于气泡地图）
        PROVINCE_CENTER = {
            '北京': [116.4074, 39.9042], '上海': [121.4737, 31.2304], '天津': [117.1902, 39.1256], '重庆': [106.5044, 29.5582],
            '河北': [114.4995, 38.1006], '山西': [112.5624, 37.8735], '内蒙古': [111.7510, 40.8415], '辽宁': [123.4315, 41.8057],
            '吉林': [125.3235, 43.8171], '黑龙江': [126.5364, 45.8022], '江苏': [118.7674, 32.0415], '浙江': [120.1551, 30.2741],
            '安徽': [117.2272, 31.8206], '福建': [119.2965, 26.0745], '江西': [115.8582, 28.6820], '山东': [117.0009, 36.6758],
            '河南': [113.6254, 34.7466], '湖北': [114.3055, 30.5931], '湖南': [112.9388, 28.2282], '广东': [113.2644, 23.1291],
            '广西': [108.3661, 22.8176], '海南': [110.1999, 20.0440], '四川': [104.0668, 30.5728], '贵州': [106.6302, 26.6477],
            '云南': [102.8329, 24.8801], '西藏': [91.1409, 29.6565], '陕西': [108.9402, 34.3416], '甘肃': [103.8343, 36.0611],
            '青海': [101.7782, 36.6232], '宁夏': [106.2309, 38.4872], '新疆': [87.6168, 43.8256]
        }
        province_sale["lon"] = province_sale["省份"].apply(lambda p: PROVINCE_CENTER.get(p, [116.4074,39.9042])[0])
        province_sale["lat"] = province_sale["省份"].apply(lambda p: PROVINCE_CENTER.get(p, [116.4074,39.9042])[1])

        # 尝试加载GeoJSON
        china_geojson = get_china_geojson()
        if china_geojson:
            try:
                PROVINCE_NAME_MAP = {
                    '北京': '北京市', '上海': '上海市', '天津': '天津市', '重庆': '重庆市',
                    '河北': '河北省', '山西': '山西省', '内蒙古': '内蒙古自治区', '辽宁': '辽宁省',
                    '吉林': '吉林省', '黑龙江': '黑龙江省', '江苏': '江苏省', '浙江': '浙江省',
                    '安徽': '安徽省', '福建': '福建省', '江西': '江西省', '山东': '山东省',
                    '河南': '河南省', '湖北': '湖北省', '湖南': '湖南省', '广东': '广东省',
                    '广西': '广西壮族自治区', '海南': '海南省', '四川': '四川省', '贵州': '贵州省',
                    '云南': '云南省', '西藏': '西藏自治区', '陕西': '陕西省', '甘肃': '甘肃省',
                    '青海': '青海省', '宁夏': '宁夏回族自治区', '新疆': '新疆维吾尔自治区'
                }
                province_sale["省份全称"] = province_sale["省份"].map(PROVINCE_NAME_MAP)
                province_sale_geo = province_sale.dropna(subset=["省份全称"])
                if not province_sale_geo.empty:
                    fig_map = px.choropleth(
                        province_sale_geo,
                        geojson=china_geojson,
                        locations="省份全称",
                        featureidkey="properties.name",
                        color="万元",
                        color_continuous_scale="Blues",
                        range_color=(0, province_sale_geo["万元"].max()),
                        hover_name="省份全称",
                        hover_data={"万元": ":,.2f"},
                        title="全国省份销售额热力图（填充地图）"
                    )
                    fig_map.update_geos(fitbounds="locations", visible=False)
                    fig_map.update_layout(margin={"r":0,"t":50,"l":0,"b":0}, height=700)
                    st.plotly_chart(fig_map, use_container_width=True)
                    st.stop()  # 成功显示后不再执行气泡图
            except Exception as e:
                if st.session_state.debug_mode:
                    st.warning(f"GeoJSON渲染失败: {e}，使用气泡地图")

        # 降级方案：气泡地图
        fig_map = px.scatter_geo(
            province_sale,
            lon="lon", lat="lat",
            size="万元", color="万元",
            hover_name="省份",
            projection="natural earth",
            title="全国省份销售额分布（气泡大小代表销售额）",
            color_continuous_scale="Blues",
            size_max=60
        )
        fig_map.update_layout(
            geo=dict(scope='asia', center=dict(lat=35, lon=105), projection_scale=1.2),
            margin={"r":0,"t":50,"l":0,"b":0},
            height=700
        )
        st.plotly_chart(fig_map, use_container_width=True)
    else:
        st.error("❌ 无法将城市映射到省份，请检查订单表中的'市区'字段")
else:
    st.info("暂无订单城市数据")

# ----------------------------- 明细核对 -----------------------------
with st.expander("📄 订单明细（核对总金额）"):
    if not df_o.empty:
        st.dataframe(df_o[["日期", "品牌", "品类", "运营中心", "市区", "订单金额"]], use_container_width=True)
        st.success(f"✅ 订单总金额：{total_amount:,.2f} 元  =  {total_wan:.2f} 万元")
    else:
        st.info("无订单明细")
