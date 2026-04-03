# ----------------------------- 品牌筛选函数（终极修复版） -----------------------------
def filter_by_brand(df, selected_brands):
    # 1. 没有选中品牌，直接返回
    if not selected_brands:
        return df.copy()
    
    # 2. 安全获取品牌列（不存在直接返回）
    brand_col = None
    if '品牌' in df.columns:
        brand_col = '品牌'
    elif '意向品牌' in df.columns:
        brand_col = '意向品牌'
    
    # 3. 没有品牌列 → 直接返回
    if brand_col is None or brand_col not in df.columns:
        return df.copy()
    
    # 4. 安全获取列数据（防止空列报错）
    try:
        brands = df[brand_col].fillna("未知").tolist()
    except:
        return df.copy()
    
    # 5. 品类列（安全获取）
    cat_col = '品类' if '品类' in df.columns else None
    try:
        cats = df[cat_col].fillna("未知").tolist() if cat_col else ['未知'] * len(df)
    except:
        cats = ['未知'] * len(df)
    
    # 6. 筛选逻辑
    keep = []
    for brand, cat in zip(brands, cats):
        flag = False
        for item in selected_brands:
            if item == '美的' and brand == '美的':
                flag = True
                break
            if item == '东芝' and brand == '东芝':
                flag = True
                break
            if item == '小天鹅' and brand == '小天鹅':
                flag = True
                break
            if item == 'COLMO' and brand == 'COLMO':
                flag = True
                break
            if item == '美的厨热' and brand == '美的' and cat == '厨热':
                flag = True
                break
            if item == '美的冰箱' and brand == '美的' and cat == '冰箱':
                flag = True
                break
            if item == '美的空调' and brand == '美的' and cat == '空调':
                flag = True
                break
            if item == '洗衣机汇总':
                if brand == '小天鹅' or (brand == '美的' and cat == '洗衣机'):
                    flag = True
                    break
        keep.append(flag)
    
    # 7. 安全返回
    try:
        return df[keep].copy()
    except:
        return df.copy()
