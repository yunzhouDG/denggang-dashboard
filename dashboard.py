import streamlit as st
import pandas as pd
import sqlite3
import zipfile
import tempfile
import os
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go

st.set_page_config(layout="wide", page_title="天猫新零售数据看板", page_icon="📊")

# ----------------------------- 内置城市经纬度（全国完整版，含福建全量城市） -----------------------------
CITY_COORDS = {
    # 直辖市
    '北京': [116.4074, 39.9042], '上海': [121.4737, 31.2304], '天津': [117.1902, 39.1256], '重庆': [106.5044, 29.5582],
    # 河北
    '石家庄': [114.5149, 38.0428], '唐山': [118.1827, 39.6307], '秦皇岛': [119.5219, 39.9307], '邯郸': [114.5261, 36.6052],
    '邢台': [114.5093, 37.0671], '保定': [115.4624, 38.8735], '张家口': [114.8722, 40.8286], '承德': [117.9327, 40.9501],
    '沧州': [116.8328, 38.3141], '廊坊': [116.6821, 39.1327], '衡水': [115.7230, 37.7401],
    # 山西
    '太原': [112.5624, 37.8735], '大同': [113.2930, 40.0761], '阳泉': [113.5699, 37.8671], '长治': [113.1269, 36.1919],
    '晋城': [112.8527, 35.4951], '朔州': [112.4305, 39.3281], '晋中': [112.7370, 37.7501], '运城': [110.9901, 35.0271],
    '忻州': [112.7317, 38.4151], '临汾': [111.5172, 36.0851], '吕梁': [111.1324, 37.5191],
    # 内蒙古
    '呼和浩特': [111.7510, 40.8415], '包头': [109.8386, 40.6591], '乌海': [106.8206, 39.6711], '赤峰': [118.9401, 42.2591],
    '通辽': [122.2658, 43.6491], '鄂尔多斯': [109.7814, 39.6111], '呼伦贝尔': [119.7541, 49.2121], '巴彦淖尔': [107.4026, 40.7441],
    '乌兰察布': [113.1258, 41.0601], '兴安盟': [122.5091, 46.0801], '锡林郭勒': [116.1001, 43.9501], '阿拉善': [105.7101, 38.8501],
    # 辽宁
    '沈阳': [123.4315, 41.8057], '大连': [121.6207, 38.9157], '鞍山': [122.9967, 41.1067], '抚顺': [123.9527, 41.8767],
    '本溪': [123.7677, 41.2967], '丹东': [124.3797, 40.1337], '锦州': [121.1317, 41.1007], '营口': [122.2317, 40.6667],
    '阜新': [121.6497, 42.0207], '辽阳': [123.1727, 41.2767], '盘锦': [122.0697, 41.1197], '铁岭': [123.8497, 42.2907],
    '朝阳': [120.4517, 41.5707], '葫芦岛': [120.8417, 40.7107],
    # 吉林
    '长春': [125.3235, 43.8171], '吉林': [126.5515, 43.8371], '四平': [124.3605, 43.1601], '辽源': [125.1305, 42.8901],
    '通化': [125.9305, 41.7301], '白山': [126.4105, 41.9401], '松原': [124.8205, 45.1201], '白城': [122.8305, 45.6201],
    '延边': [129.5005, 42.9001],
    # 黑龙江
    '哈尔滨': [126.5364, 45.8022], '齐齐哈尔': [123.9644, 47.3422], '鸡西': [130.9944, 45.2922], '鹤岗': [130.2844, 47.3322],
    '双鸭山': [131.1744, 46.6422], '大庆': [125.1344, 46.5822], '伊春': [128.9344, 47.7222], '佳木斯': [130.3744, 46.8122],
    '七台河': [130.9844, 45.8022], '牡丹江': [129.6344, 44.5822], '黑河': [127.5344, 50.2322], '绥化': [126.9844, 46.6322],
    '大兴安岭': [124.7244, 52.9722],
    # 江苏
    '南京': [118.7674, 32.0415], '无锡': [120.3136, 31.4908], '徐州': [117.1387, 34.2633], '常州': [119.9740, 31.8107],
    '苏州': [120.5853, 31.2989], '南通': [120.8646, 32.0167], '连云港': [119.1629, 34.5927], '淮安': [119.0212, 33.6268],
    '盐城': [120.1399, 33.3776], '扬州': [119.4256, 32.3937], '镇江': [119.4529, 32.2044], '泰州': [119.9078, 32.4588],
    '宿迁': [118.2751, 33.9630],
    # 浙江
    '杭州': [120.1551, 30.2741], '宁波': [121.5438, 29.8683], '温州': [120.6994, 27.9949], '嘉兴': [120.7508, 30.7536],
    '湖州': [120.0997, 30.8703], '绍兴': [120.5818, 30.0082], '金华': [119.6407, 29.0895], '衢州': [118.8803, 28.9703],
    '舟山': [122.2072, 29.9856], '台州': [121.4208, 28.6563], '丽水': [119.9152, 28.4527],
    # 安徽
    '合肥': [117.2272, 31.8206], '芜湖': [118.3830, 31.3533], '蚌埠': [117.3583, 32.9183], '淮南': [116.9985, 32.6476],
    '马鞍山': [118.5022, 31.6907], '淮北': [116.7947, 33.9617], '铜陵': [117.8139, 30.9468], '安庆': [117.0366, 30.5246],
    '黄山': [118.2967, 29.7157], '滁州': [118.3161, 32.3127], '阜阳': [115.8189, 32.8937], '宿州': [116.9781, 33.6363],
    '六安': [116.4964, 31.7354], '亳州': [115.7773, 33.8715], '池州': [117.4897, 30.6607], '宣城': [118.7333, 30.9508],
    # 福建（含泉州及下属县级市，核心修复）
    '福州': [119.2965, 26.0745], '厦门': [118.0895, 24.4798], '莆田': [119.0025, 25.4405], '三明': [117.6405, 26.2505],
    '泉州': [118.5894, 24.9089], '漳州': [117.0405, 24.5105], '南平': [118.1605, 26.6405], '龙岩': [117.0205, 25.0705],
    '宁德': [119.5205, 26.6605], '晋江': [118.5768, 24.8267], '石狮': [118.6437, 24.7328], '南安': [118.3884, 24.9568],
    '惠安': [118.7868, 25.0367], '安溪': [118.1868, 25.0667], '永春': [118.2868, 25.3267], '德化': [118.2468, 25.4967],
    # 江西
    '南昌': [115.8582, 28.6820], '景德镇': [117.2102, 29.2902], '萍乡': [113.8502, 27.6102], '九江': [115.9902, 29.7102],
    '新余': [114.9302, 27.8102], '鹰潭': [117.0302, 28.2502], '赣州': [114.9302, 25.8502], '吉安': [114.9702, 27.1102],
    '宜春': [114.3902, 27.8102], '抚州': [116.3502, 27.9502], '上饶': [117.9702, 28.4502],
    # 山东
    '济南': [117.0009, 36.6758], '青岛': [120.3826, 36.0671], '淄博': [118.0409, 36.8001], '枣庄': [117.5709, 34.8601],
    '东营': [118.4909, 37.4601], '烟台': [121.3909, 37.5301], '潍坊': [119.1009, 36.7701], '济宁': [116.5909, 35.4201],
    '泰安': [117.1309, 36.1901], '威海': [122.1309, 37.5101], '日照': [119.5309, 35.4201], '临沂': [118.3509, 35.0501],
    '德州': [116.2909, 37.4501], '聊城': [115.9709, 36.4501], '滨州': [118.0309, 37.3601], '菏泽': [115.4809, 35.1401],
    # 河南
    '郑州': [113.6254, 34.7466], '开封': [114.3054, 34.7966], '洛阳': [112.4554, 34.6266], '平顶山': [113.2954, 33.7466],
    '安阳': [114.3254, 36.0966], '鹤壁': [114.2954, 35.9066], '新乡': [113.8854, 35.3066], '焦作': [113.2154, 35.2466],
    '濮阳': [115.0354, 35.7766], '许昌': [113.8154, 34.0266], '漯河': [114.0254, 33.5766], '三门峡': [111.1954, 34.7766],
    '南阳': [112.5354, 33.0066], '商丘': [115.6554, 34.4466], '信阳': [114.0954, 32.1366], '周口': [114.6354, 33.6266],
    '驻马店': [114.0254, 32.9866], '济源': [112.6054, 35.0866],
    # 湖北
    '武汉': [114.3055, 30.5931], '黄石': [115.0355, 30.2031], '十堰': [110.7955, 32.6231], '宜昌': [111.2955, 30.7031],
    '襄阳': [112.1455, 32.0431], '鄂州': [114.8955, 30.3931], '荆门': [112.2055, 31.0331], '孝感': [113.9155, 30.9231],
    '荆州': [112.2455, 30.3331], '黄冈': [114.8755, 30.4531], '咸宁': [114.3055, 29.8631], '随州': [113.3755, 31.6931],
    '恩施': [109.4855, 30.2731], '仙桃': [113.4555, 30.3731], '潜江': [112.8955, 30.4231], '天门': [113.1655, 30.6631],
    # 湖南
    '长沙': [112.9388, 28.2282], '株洲': [113.1588, 27.8482], '湘潭': [112.9188, 27.8782], '衡阳': [112.5788, 26.9082],
    '邵阳': [111.4688, 27.2482], '岳阳': [113.1388, 29.3782], '常德': [111.6988, 29.0382], '张家界': [110.4888, 29.1282],
    '益阳': [112.3388, 28.6082], '郴州': [113.0288, 25.7982], '永州': [111.6288, 26.4282], '怀化': [109.9788, 27.5682],
    '娄底': [112.0088, 27.7382], '湘西': [109.7388, 28.3182],
    # 广东
    '广州': [113.2644, 23.1291], '韶关': [113.5944, 24.8091], '深圳': [114.0579, 22.5431], '珠海': [113.5744, 22.2791],
    '汕头': [116.6944, 23.3991], '佛山': [113.1224, 23.0215], '江门': [113.0844, 22.5991], '湛江': [110.3644, 21.2791],
    '茂名': [110.9244, 21.6691], '肇庆': [112.4444, 23.0591], '惠州': [114.4144, 23.0991], '梅州': [116.1144, 24.2991],
    '汕尾': [115.3744, 22.7791], '河源': [114.6944, 23.7491], '阳江': [111.9744, 21.8591], '清远': [113.0544, 23.6891],
    '东莞': [113.7518, 23.0205], '中山': [113.3844, 22.5291], '潮州': [116.6344, 23.6591], '揭阳': [116.3544, 23.5591],
    '云浮': [112.0244, 22.9391],
    # 广西
    '南宁': [108.3661, 22.8176], '柳州': [109.4161, 24.3976], '桂林': [110.2961, 25.2776], '梧州': [111.3461, 23.4776],
    '北海': [109.1261, 21.4876], '防城港': [108.3561, 21.6976], '钦州': [108.6461, 21.9676], '贵港': [109.6161, 23.1176],
    '玉林': [110.1561, 22.6276], '百色': [106.6261, 23.9076], '贺州': [111.5561, 24.4176], '河池': [108.0561, 24.7076],
    '来宾': [109.2261, 23.7676], '崇左': [107.3761, 22.4176],
    # 海南
    '海口': [110.1999, 20.0440], '三亚': [109.5099, 18.2500], '三沙': [112.3499, 16.8300], '儋州': [109.5799, 19.5200],
    '文昌': [110.7299, 19.6100], '琼海': [110.4699, 19.2500], '万宁': [110.3999, 18.8000], '东方': [108.6499, 19.0900],
    # 四川
    '成都': [104.0668, 30.5728], '自贡': [104.7768, 29.3428], '攀枝花': [101.7168, 26.5828], '泸州': [105.4368, 28.8928],
    '德阳': [104.2368, 31.1328], '绵阳': [104.6768, 31.4728], '广元': [105.8468, 32.4428], '遂宁': [105.5768, 30.5228],
    '内江': [105.0668, 29.5928], '乐山': [103.7468, 29.5628], '南充': [106.0868, 30.7928], '眉山': [103.8468, 30.0528],
    '宜宾': [104.6368, 29.7628], '广安': [106.6368, 30.4628], '达州': [107.4768, 31.2128], '雅安': [103.0068, 30.0028],
    '巴中': [106.7668, 31.8628], '资阳': [104.6668, 30.1028], '阿坝': [102.8268, 32.1028], '甘孜': [100.0068, 30.0428],
    '凉山': [102.2668, 27.8928],
    # 贵州
    '贵阳': [106.6302, 26.6477], '六盘水': [104.8302, 26.6077], '遵义': [106.9302, 27.7277], '安顺': [105.9302, 26.2577],
    '毕节': [105.2902, 27.3077], '铜仁': [109.1902, 27.7377], '黔西南': [104.9002, 25.0977], '黔东南': [107.9802, 26.5877],
    '黔南': [107.5102, 26.2677],
    # 云南
    '昆明': [102.8329, 24.8801], '曲靖': [103.7929, 25.4901], '玉溪': [102.5529, 24.3501], '保山': [99.1729, 25.1201],
    '昭通': [103.7229, 27.3301], '丽江': [100.2329, 26.8601], '普洱': [100.9729, 22.8301], '临沧': [100.0829, 23.8801],
    '楚雄': [101.5229, 25.0201], '红河': [102.4929, 23.3701], '文山': [104.2429, 23.4001], '西双版纳': [100.8029, 22.0001],
    '大理': [100.2629, 25.5801], '德宏': [98.5829, 24.4401], '怒江': [98.8629, 25.8501], '迪庆': [99.7029, 27.8201],
    # 西藏
    '拉萨': [91.1409, 29.6565], '日喀则': [88.8809, 29.2665], '昌都': [97.1709, 31.1465], '林芝': [94.3509, 29.6765],
    '山南': [91.7709, 29.2365], '那曲': [92.0509, 31.4765], '阿里': [80.0009, 32.5065],
    # 陕西
    '西安': [108.9402, 34.3416], '铜川': [109.0702, 35.0916], '宝鸡': [107.1402, 34.3616], '咸阳': [108.7202, 34.3316],
    '渭南': [109.5002, 34.5116], '延安': [109.4802, 36.5916], '汉中': [107.0202, 33.0716], '榆林': [109.7402, 38.2916],
    '安康': [109.0202, 32.6916], '商洛': [109.9402, 33.8716],
    # 甘肃
    '兰州': [103.8343, 36.0611], '嘉峪关': [98.2243, 39.8011], '金昌': [102.1843, 38.5211], '白银': [104.1443, 36.5511],
    '天水': [105.7243, 34.5711], '武威': [102.6343, 37.9311], '张掖': [100.4543, 38.9311], '平凉': [106.6743, 35.5411],
    '酒泉': [98.5043, 39.7211], '庆阳': [107.6443, 35.7111], '定西': [104.6243, 35.5811], '陇南': [104.9243, 33.4011],
    '临夏': [103.2043, 35.6011], '甘南': [102.9143, 34.9811],
    # 青海
    '西宁': [101.7782, 36.6232], '海东': [102.0082, 36.4932], '海北': [100.9382, 37.0032], '黄南': [101.2082, 35.5032],
    '海南': [100.6282, 36.2032], '果洛': [100.1082, 34.5032], '玉树': [96.9782, 33.0032], '海西': [97.3782, 37.3032],
    # 宁夏
    '银川': [106.2309, 38.4872], '石嘴山': [106.3909, 39.0372], '吴忠': [106.2009, 37.9072], '固原': [106.2809, 36.0172],
    '中卫': [105.1909, 37.5172],
    # 新疆
    '乌鲁木齐': [87.6168, 43.8256], '克拉玛依': [84.7768, 45.5956], '吐鲁番': [89.1968, 42.9556], '哈密': [93.4468, 42.8056],
    '昌吉': [87.3168, 44.0256], '博尔塔拉': [82.0768, 44.9056], '巴音郭楞': [86.0768, 41.6856], '阿克苏': [80.2968, 41.1756],
    '克孜勒苏': [76.1768, 39.3856], '喀什': [75.5968, 39.4756], '和田': [79.9468, 37.1256], '伊犁': [81.3368, 43.9256],
    '塔城': [82.9768, 46.7456], '阿勒泰': [88.1268, 47.8456], '石河子': [85.9468, 44.3256]
}
DEFAULT_COORD = [116.4074, 39.9042]

# ----------------------------- 城市名自动清洗+坐标匹配（核心修复：解决泉州错位） -----------------------------
def get_city_coord(city_name):
    if not city_name or pd.isna(city_name):
        return DEFAULT_COORD
    # 1. 统一字符串格式
    city_str = str(city_name).strip()
    # 2. 自动去除省、市、区、县等后缀，解决"泉州市"匹配不到"泉州"的问题
    replace_suffix = ["省", "市", "区", "县", "自治州", "壮族自治区", "回族自治区", "维吾尔自治区", "特别行政区"]
    for suffix in replace_suffix:
        city_str = city_str.replace(suffix, "")
    # 3. 去除多余空格
    city_str = city_str.strip()
    # 4. 匹配经纬度，未匹配则返回默认坐标
    return CITY_COORDS.get(city_str, DEFAULT_COORD)

# ----------------------------- 数据加载（完全适配原始表结构） -----------------------------
@st.cache_data(ttl=86400)
def load_data():
    with zipfile.ZipFile('data.zip', 'r') as z:
        db_files = [f for f in z.namelist() if f.endswith('.db')]
        if not db_files:
            st.error("data.zip 中没有找到 .db 文件")
            st.stop()
        with tempfile.NamedTemporaryFile(delete=False, suffix='.db') as tmp:
            with z.open(db_files[0]) as source:
                tmp.write(source.read())
            tmp_path = tmp.name
    conn = sqlite3.connect(tmp_path)
    df_main = pd.read_sql("SELECT * FROM 客资明细表", conn)
    df_order = pd.read_sql("SELECT * FROM 订单表", conn)
    conn.close()
    os.unlink(tmp_path)

    # 打印原始列名，方便调试
    import sys
    print("客资表原始列名:", df_main.columns.tolist(), file=sys.stderr)
    print("订单表原始列名:", df_order.columns.tolist(), file=sys.stderr)

    # 客资表日期处理：获取时间 -> 日期
    if '获取时间' in df_main.columns:
        df_main['日期'] = pd.to_datetime(df_main['获取时间'], errors='coerce')
    else:
        df_main['日期'] = pd.NaT

    # 订单表日期处理
    if '日期' in df_order.columns:
        df_order['日期'] = pd.to_datetime(df_order['日期'], errors='coerce')
    else:
        df_order['日期'] = pd.NaT

    # 订单金额处理
    df_order['订单金额'] = pd.to_numeric(df_order['订单金额'], errors='coerce').fillna(0)

    # 客资表字段补全
    if '意向品牌' in df_main.columns:
        df_main['品牌'] = df_main['意向品牌'].fillna('未知')
    else:
        df_main['品牌'] = '未知'
    if '品类' in df_main.columns:
        df_main['品类'] = df_main['品类'].fillna('未知')
    else:
        df_main['品类'] = '未知'
    if '运营中心' in df_main.columns:
        df_main['运营中心'] = df_main['运营中心'].fillna('未知')
    else:
        df_main['运营中心'] = '未知'
    df_main['片区'] = '未知'  # 客资表无片区，补全
    if '外呼状态' in df_main.columns:
        df_main['外呼状态'] = df_main['外呼状态'].fillna('')
    else:
        df_main['外呼状态'] = ''
    if '最新跟进状态' in df_main.columns:
        df_main['最新跟进状态'] = df_main['最新跟进状态'].fillna('')
    else:
        df_main['最新跟进状态'] = ''

    # 订单表字段补全
    if '品牌' in df_order.columns:
        df_order['品牌'] = df_order['品牌'].fillna('未知')
    else:
        df_order['品牌'] = '未知'
    if '品类' in df_order.columns:
        df_order['品类'] = df_order['品类'].fillna('未知')
    else:
        df_order['品类'] = '未知'
    if '运中' in df_order.columns:
        df_order['运营中心'] = df_order['运中'].fillna('未知')
    else:
        df_order['运营中心'] = '未知'
    if '片区' in df_order.columns:
        df_order['片区'] = df_order['片区'].fillna('未知')
    else:
        df_order['片区'] = '未知'
    if '市区' in df_order.columns:
        df_order['市区'] = df_order['市区'].fillna('')
    else:
        df_order['市区'] = ''

    # 删除重复列
    df_main = df_main.loc[:, ~df_main.columns.duplicated()]
    df_order = df_order.loc[:, ~df_order.columns.duplicated()]

    return df_main, df_order

df_main, df_order = load_data()

# ----------------------------- 侧边栏诊断信息 -----------------------------
st.sidebar.subheader("📋 数据诊断")
st.sidebar.write(f"客资表行数: {len(df_main)}")
st.sidebar.write(f"订单表行数: {len(df_order)}")
st.sidebar.write("客资表关键列:", [c for c in df_main.columns if c in ['日期','品牌','品类','运营中心','外呼状态','最新跟进状态']])
st.sidebar.write("订单表关键列:", [c for c in df_order.columns if c in ['日期','品牌','品类','运营中心','市区','订单金额']])
if '日期' in df_main.columns:
    st.sidebar.write(f"客资表日期范围: {df_main['日期'].min()} 至 {df_main['日期'].max()}")
else:
    st.sidebar.error("客资表中没有日期列！")

with st.sidebar.expander("预览客资表（前5行）"):
    st.dataframe(df_main[['日期','品牌','品类','运营中心','外呼状态','最新跟进状态']].head())
with st.sidebar.expander("预览订单表（前5行）"):
    st.dataframe(df_order[['日期','品牌','品类','运营中心','市区','订单金额']].head())

# ----------------------------- 工具函数 -----------------------------
def get_unique_sorted(series):
    if series.empty:
        return []
    return sorted(series.dropna().unique())

def filter_by_brand(df, brand_selections):
    if df.empty or not brand_selections:
        return df.copy()
    if '品牌' not in df.columns:
        return df.copy()
    df = df.reset_index(drop=True).copy()
    has_cat = '品类' in df.columns
    df['品牌'] = df['品牌'].fillna('未知')
    if has_cat:
        df['品类'] = df['品类'].fillna('未知')
    mask = [False] * len(df)
    for idx, row in enumerate(df.itertuples(index=False)):
        brand = getattr(row, '品牌')
        cat = getattr(row, '品类') if has_cat else None
        for b in brand_selections:
            if b == '美的' and brand == '美的':
                mask[idx] = True
                break
            elif b == '东芝' and brand == '东芝':
                mask[idx] = True
                break
            elif b == '小天鹅' and brand == '小天鹅':
                mask[idx] = True
                break
            elif b == 'COLMO' and brand == 'COLMO':
                mask[idx] = True
                break
            elif b == '美的厨热' and has_cat and brand == '美的' and cat == '厨热':
                mask[idx] = True
                break
            elif b == '美的冰箱' and has_cat and brand == '美的' and cat == '冰箱':
                mask[idx] = True
                break
            elif b == '美的空调' and has_cat and brand == '美的' and cat == '空调':
                mask[idx] = True
                break
            elif b == '洗衣机汇总' and (brand == '小天鹅' or (has_cat and brand == '美的' and cat == '洗衣机')):
                mask[idx] = True
                break
    return df[mask].copy()

# ----------------------------- 侧边栏筛选 -----------------------------
st.sidebar.header("🔍 数据筛选")

# 日期范围
if '日期' in df_main.columns and not df_main['日期'].isna().all():
    min_date = df_main['日期'].min().date()
    max_date = df_main['日期'].max().date()
    date_range = st.sidebar.date_input("日期范围", [min_date, max_date])
else:
    date_range = [datetime.today().date(), datetime.today().date()]
    st.sidebar.warning("客资表缺少有效日期列，日期筛选无效")

brand_options = ['美的', '东芝', '小天鹅', 'COLMO', '美的厨热', '美的冰箱', '美的空调', '洗衣机汇总']
selected_brands = st.sidebar.multiselect("品牌", brand_options, default=brand_options)

category_options = get_unique_sorted(df_main['品类']) if '品类' in df_main.columns else []
selected_categories = st.sidebar.multiselect("品类", category_options, default=category_options)

region_options = get_unique_sorted(df_main['片区']) if '片区' in df_main.columns else []
selected_regions = st.sidebar.multiselect("片区", region_options, default=region_options)

center_options = get_unique_sorted(df_main['运营中心']) if '运营中心' in df_main.columns else []
selected_centers = st.sidebar.multiselect("运营中心", center_options, default=center_options)

# ----------------------------- 筛选函数 -----------------------------
def filter_main(df, date_range, categories, regions, centers):
    if '日期' in df.columns and len(date_range) == 2:
        s, e = date_range
        df = df[(df['日期'].dt.date >= s) & (df['日期'].dt.date <= e)]
    if categories and '品类' in df.columns:
        df = df[df['品类'].isin(categories)]
    if regions and '片区' in df.columns:
        df = df[df['片区'].isin(regions)]
    if centers and '运营中心' in df.columns:
        df = df[df['运营中心'].isin(centers)]
    return df

def filter_order(df, date_range, categories, centers):
    if '日期' in df.columns and len(date_range) == 2:
        s, e = date_range
        df = df[(df['日期'].dt.date >= s) & (df['日期'].dt.date <= e)]
    if categories and '品类' in df.columns:
        df = df[df['品类'].isin(categories)]
    if centers and '运营中心' in df.columns:
        df = df[df['运营中心'].isin(centers)]
    return df

df_main_filtered = filter_main(df_main, date_range, selected_categories, selected_regions, selected_centers)
df_main_filtered = filter_by_brand(df_main_filtered, selected_brands)

df_order_filtered = filter_order(df_order, date_range, selected_categories, selected_centers)
df_order_filtered = filter_by_brand(df_order_filtered, selected_brands)

st.sidebar.markdown("---")
st.sidebar.write(f"筛选后客资数: {len(df_main_filtered)}")
st.sidebar.write(f"筛选后订单数: {len(df_order_filtered)}")

# ----------------------------- 指标卡片 -----------------------------
st.title("🏬 天猫新零售数据看板")
col1, col2, col3, col4 = st.columns(4)

total_leads = len(df_main_filtered)
valid_leads = len(df_main_filtered[df_main_filtered['外呼状态'].isin(['高意向','低意向','无需外呼'])]) if '外呼状态' in df_main_filtered.columns else 0
total_orders = len(df_order_filtered)
total_amount = df_order_filtered['订单金额'].sum() if not df_order_filtered.empty else 0

col1.metric("总客资数", f"{total_leads:,}")
col2.metric("有效客资数", f"{valid_leads:,}")
col3.metric("成交数", f"{total_orders:,}")
col4.metric("总金额", f"{total_amount:,.0f} 元")

if total_leads == 0 and total_orders == 0:
    st.warning("当前筛选条件下没有数据，请调整筛选条件或检查数据源。")
    st.stop()

# ----------------------------- 环比计算 -----------------------------
def calc_change(c, p):
    return (c-p)/p if p != 0 else None

today = datetime.today().date()
yesterday = today - timedelta(days=1)
amt_today = df_order_filtered[df_order_filtered['日期'].dt.date == today]['订单金额'].sum() if not df_order_filtered.empty else 0
amt_yes = df_order_filtered[df_order_filtered['日期'].dt.date == yesterday]['订单金额'].sum() if not df_order_filtered.empty else 0
dc = calc_change(amt_today, amt_yes)
if dc is not None:
    st.sidebar.metric("日环比", f"{dc:.1%}")

first_cur = datetime(today.year, today.month, 1).date()
if today.month == 1:
    first_prev = datetime(today.year-1, 12, 1).date()
else:
    first_prev = datetime(today.year, today.month-1, 1).date()
amt_cur_m = df_order_filtered[df_order_filtered['日期'].dt.date >= first_cur]['订单金额'].sum() if not df_order_filtered.empty else 0
amt_pre_m = df_order_filtered[(df_order_filtered['日期'].dt.date >= first_prev) & (df_order_filtered['日期'].dt.date < first_cur)]['订单金额'].sum() if not df_order_filtered.empty else 0
mc = calc_change(amt_cur_m, amt_pre_m)
if mc is not None:
    st.sidebar.metric("月环比", f"{mc:.1%}")

# ----------------------------- 转化漏斗 -----------------------------
st.header("📉 转化漏斗")

def funnel(main, order):
    t = len(main)
    if '外呼状态' not in main.columns:
        v = a = f = 0
    else:
        valid_mask = main['外呼状态'].isin(['高意向','低意向','无需外呼'])
        v = valid_mask.sum()
        if '最新跟进状态' not in main.columns:
            a = f = 0
        else:
            allocated_mask = valid_mask & (~main['最新跟进状态'].isin(['未分配']))
            a = allocated_mask.sum()
            followed_mask = allocated_mask & (~main['最新跟进状态'].isin(['未分配','待查看','待联系']))
            f = followed_mask.sum()
    o = len(order)
    return [t, v, a, f, o]

stages = ["总客资", "有效客资", "分配数", "跟进数", "成交数"]
vals = funnel(df_main_filtered, df_order_filtered)
fig_funnel = go.Figure(go.Funnel(y=stages, x=vals, textinfo="value+percent initial"))
st.plotly_chart(fig_funnel, use_container_width=True)

# ----------------------------- 转化率趋势 -----------------------------
st.header("📈 转化率趋势")
if not df_main_filtered.empty and '日期' in df_main_filtered.columns:
    daily = df_main_filtered.groupby(df_main_filtered['日期'].dt.date).apply(
        lambda x: pd.Series({
            '总客资': len(x),
            '有效客资': len(x[x['外呼状态'].isin(['高意向','低意向','无需外呼'])]) if '外呼状态' in x.columns else 0,
            '分配数': len(x[(x['外呼状态'].isin(['高意向','低意向','无需外呼'])) & (~x['最新跟进状态'].isin(['未分配']))]) if all(c in x.columns for c in ['外呼状态','最新跟进状态']) else 0,
            '跟进数': len(x[(x['外呼状态'].isin(['高意向','低意向','无需外呼'])) & (~x['最新跟进状态'].isin(['未分配','待查看','待联系']))]) if all(c in x.columns for c in ['外呼状态','最新跟进状态']) else 0
        })
    ).reset_index()
    d_ord = df_order_filtered.groupby(df_order_filtered['日期'].dt.date).size().reset_index(name='成交数') if not df_order_filtered.empty else pd.DataFrame(columns=['日期','成交数'])
    daily = daily.merge(d_ord, on='日期', how='left').fillna(0)
    daily['有效率'] = daily['有效客资'] / daily['总客资'].replace(0, pd.NA)
    daily['分配率'] = daily['分配数'] / daily['有效客资'].replace(0, pd.NA)
    daily['跟进率'] = daily['跟进数'] / daily['分配数'].replace(0, pd.NA)
    daily['成交率'] = daily['成交数'] / daily['跟进数'].replace(0, pd.NA)

    fig_trend = go.Figure()
    fig_trend.add_trace(go.Scatter(x=daily['日期'], y=daily['有效率'], name='有效率', mode='lines+markers'))
    fig_trend.add_trace(go.Scatter(x=daily['日期'], y=daily['分配率'], name='分配率', mode='lines+markers'))
    fig_trend.add_trace(go.Scatter(x=daily['日期'], y=daily['跟进率'], name='跟进率', mode='lines+markers'))
    fig_trend.add_trace(go.Scatter(x=daily['日期'], y=daily['成交率'], name='成交率', mode='lines+markers'))
    fig_trend.update_layout(yaxis_tickformat=".0%", yaxis_title="比率", xaxis_title="日期")
    st.plotly_chart(fig_trend, use_container_width=True)
else:
    st.info("客资数据为空或缺少日期列，无法绘制趋势图。")

# ----------------------------- 各品牌销售额 -----------------------------
st.header("📊 各品牌销售额")
if not df_order_filtered.empty and '品牌' in df_order_filtered.columns:
    bs = df_order_filtered.groupby('品牌')['订单金额'].sum().sort_values(ascending=False).head(10).reset_index()
    st.plotly_chart(px.bar(bs, x='品牌', y='订单金额', color='订单金额', title="Top10 品牌销售额"), use_container_width=True)
else:
    st.info("订单数据为空或缺少品牌列。")

st.subheader("销售额分布")
t1, t2, t3 = st.tabs(["品类", "片区", "运营中心"])
with t1:
    if not df_order_filtered.empty and '品类' in df_order_filtered.columns:
        c = df_order_filtered.groupby('品类')['订单金额'].sum().reset_index()
        st.plotly_chart(px.bar(c, x='品类', y='订单金额', title="品类销售额"), use_container_width=True)
    else:
        st.info("无品类数据")
with t2:
    if not df_order_filtered.empty and '片区' in df_order_filtered.columns:
        r = df_order_filtered.groupby('片区')['订单金额'].sum().reset_index()
        st.plotly_chart(px.bar(r, x='片区', y='订单金额', title="片区销售额"), use_container_width=True)
    else:
        st.info("无片区数据")
with t3:
    if not df_order_filtered.empty and '运营中心' in df_order_filtered.columns:
        ce = df_order_filtered.groupby('运营中心')['订单金额'].sum().sort_values(ascending=False).head(15).reset_index()
        st.plotly_chart(px.bar(ce, x='运营中心', y='订单金额', title="运营中心销售额"), use_container_width=True)
    else:
        st.info("无运营中心数据")

# ----------------------------- 中国地图热力图（已修复泉州错位问题） -----------------------------
st.header("🗺️ 中国地图 - 城市销售额热力图")
if not df_order_filtered.empty and '市区' in df_order_filtered.columns:
    # 过滤空值、未知值
    city_data = df_order_filtered[
        df_order_filtered['市区'].notna() & 
        (df_order_filtered['市区'] != '') & 
        (df_order_filtered['市区'] != '未知')
    ]
    if city_data.empty:
        st.warning("订单表中「市区」字段全部为空，无法绘制地图。")
    else:
        # 按城市汇总销售额
        city_amount = city_data.groupby('市区')['订单金额'].sum().reset_index()
        city_amount = city_amount.sort_values('订单金额', ascending=False)
        # 匹配经纬度（自动清洗城市名）
        city_amount['经度'] = city_amount['市区'].apply(lambda x: get_city_coord(x)[0])
        city_amount['纬度'] = city_amount['市区'].apply(lambda x: get_city_coord(x)[1])
        # 去重，限制最多100个城市
        unique_cities = city_amount.drop_duplicates(subset=['市区'])
        if len(unique_cities) > 100:
            unique_cities = unique_cities.head(100)
        
        # 检查未匹配到的城市（调试用）
        unmatched = unique_cities[
            (unique_cities['经度'] == DEFAULT_COORD[0]) & 
            (unique_cities['纬度'] == DEFAULT_COORD[1])
        ]
        if len(unmatched) > 0:
            st.warning(f"⚠️ 有 {len(unmatched)} 个城市未匹配到经纬度，已默认显示在北京：{', '.join(unmatched['市区'].tolist()[:10])}")
        
        # 绘制地图
        fig_map = px.scatter_mapbox(
            unique_cities,
            lat='纬度',
            lon='经度',
            size='订单金额',
            color='订单金额',
            color_continuous_scale='YlOrRd',
            size_max=40,
            zoom=3,
            mapbox_style='open-street-map',
            text='市区',
            hover_name='市区',
            hover_data={'订单金额': ':,.0f', '纬度': False, '经度': False},
            title='全国各市订单金额分布（气泡大小/颜色代表金额）'
        )
        fig_map.update_layout(margin=dict(l=0, r=0, t=40, b=0), height=600)
        st.plotly_chart(fig_map, use_container_width=True)
        
        with st.expander("查看详细数据"):
            st.dataframe(unique_cities[['市区', '订单金额']].style.format({'订单金额': '{:,.0f}'}))
else:
    st.warning("订单表中没有找到「市区」字段，无法绘制地图。")

st.caption("数据实时更新 | 有效客资：高/低意向/无需外呼")
