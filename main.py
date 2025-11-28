import re
import time
import random
import requests
import pandas as pd
import json
from bs4 import BeautifulSoup

# ================= 設定區 =================
BASE_URL = "https://cis.ncu.edu.tw"
ENTRY_URL = "https://cis.ncu.edu.tw/Course/main/query/byClass"
OUT_JSON = "courses_processed.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "zh-TW,zh;q=0.9"
}

# ================= 對照表 =================
PERIODS = ['1', '2', '3', '4', 'Z', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D']
PERIOD_MAP = {p: i for i, p in enumerate(PERIODS)}
DAYS_LIST = ['日', '一', '二', '三', '四', '五', '六']
DAY_MAP_FOR_PARSER = {'一':0, '二':1, '三':2, '四':3, '五':4, '六':5, '日':6}

# ================= 第一階段：目錄解析 =================
def get_all_class_links():
    print("1. 正在取得系所班級目錄...")
    try:
        r = requests.get(ENTRY_URL, headers=HEADERS, timeout=15)
        soup = BeautifulSoup(r.content, "html.parser")
        links = []
        dept_uls = soup.find_all("ul", id=re.compile(r"^dept"))
        
        for ul in dept_uls:
            parent_li = ul.find_parent("li")
            if not parent_li: continue
            dept_link = parent_li.find("a")
            if not dept_link: continue
            
            # 系所名稱 (例如: 機械工程學系)
            dept_name = re.sub(r'\(\d+\)$', '', dept_link.get_text(strip=True))
            
            class_anchors = ul.find_all("a", href=True)
            for a in class_anchors:
                href = a['href']
                # 班級名稱 (例如: 一年級 / 通識選修-人文...)
                grade_name = re.sub(r'\s*\(\d+\)$', '', a.get_text(strip=True))
                
                if "openUnion" in href:
                    full_url = BASE_URL + href if href.startswith("/") else href
                    if "show=table" not in full_url: full_url += "&show=table"
                    links.append({ "dept": dept_name, "grade": grade_name, "url": full_url })
        print(f"   共找到 {len(links)} 個班級連結。")
        return links
    except Exception as e:
        print(f"目錄取得失敗: {e}")
        return []

# ================= 第二階段：爬蟲 =================
def scrape_table_page(target):
    try:
        r = requests.get(target['url'], headers=HEADERS, timeout=20)
        if r.status_code != 200: return []
        soup = BeautifulSoup(r.content, "html.parser")
        table = soup.find("table", class_="t4")
        if not table: return []

        rows = []
        for tr in table.find_all("tr"):
            tds = tr.find_all("td")
            if len(tds) < 17: continue
            try:
                serial = tds[1].get_text(strip=True)
                if not serial.isdigit(): continue

                course_code = tds[2].get_text(strip=True)
                raw_name = tds[4].decode_contents()
                name = BeautifulSoup(raw_name.split('<br')[0], "html.parser").get_text(strip=True)
                teacher = tds[5].get_text(strip=True)
                required = tds[6].get_text(strip=True)
                credits = tds[7].get_text(strip=True)
                
                time_parts = []
                for i, day_idx in enumerate(range(10, 17)):
                    txt = tds[day_idx].get_text(" ", strip=True)
                    if txt: time_parts.append(f"{DAYS_LIST[i]}{txt.replace(' ', '/')}")
                full_time = " ".join(time_parts)

                # 分發條件
                c_html = tds[17].decode_contents()
                c_clean = re.sub(r'<br\s*/?>', ' | ', c_html)
                c_text = BeautifulSoup(c_clean, "html.parser").get_text().replace("分發條件", "").strip()
                if c_text.startswith('|'): c_text = c_text[1:].strip()

                rows.append({
                    "課程編號": serial,
                    "課程代碼": course_code,
                    "課程名稱": name,
                    "授課教師": teacher,
                    "學分": credits,
                    "必選修": required,
                    "上課時間/教室": full_time,
                    "分發條件": c_text,
                    # [KEY] 保留系所與班級資訊
                    "dept_name": target['dept'],
                    "class_name": target['grade']
                })
            except: continue
        return rows
    except: return []

# ================= 第三階段：Parser =================
def parse_time_string(time_str):
    if not time_str or str(time_str).strip() == "未定": return []
    clean_str = re.sub(r'[\(\[].*?[\)\]]', '', str(time_str))
    clean_str = re.sub(r'\/[^\s,]+', '', clean_str)
    blocks = []
    matches = re.finditer(r'([一二三四五六日])\s*([0-9A-Z,\-~]+)', clean_str)
    for match in matches:
        day_idx = DAY_MAP_FOR_PARSER.get(match.group(1))
        if day_idx is None: continue
        indices = []
        for part in match.group(2).split(','):
            part = part.strip()
            if '-' in part or '~' in part:
                b = re.split(r'[-~]', part)
                if len(b)>=2 and b[0] in PERIOD_MAP and b[1] in PERIOD_MAP:
                    s,e = PERIOD_MAP[b[0]], PERIOD_MAP[b[1]]
                    for k in range(min(s,e), max(s,e)+1): indices.append(k)
            else:
                for c in part:
                    if c in PERIOD_MAP: indices.append(PERIOD_MAP[c])
        indices = sorted(list(set(indices)))
        if indices:
            s = p = indices[0]
            for i in range(1, len(indices)):
                if indices[i] == p+1: p = indices[i]
                else:
                    blocks.append({'day': day_idx, 'start': s, 'end': p})
                    s = p = indices[i]
            blocks.append({'day': day_idx, 'start': s, 'end': p})
    return blocks

def parse_criteria_text(text):
    if not text or str(text).strip() in ("無", "None", ""): return []
    rules = []
    groups = re.split(r'\s*[|｜]\s*', str(text).strip())
    for g in groups:
        if not g.strip(): continue
        pri = 1
        m = re.match(r'^[\(\[\{]?(\d+)[\)\]\}]?\s*[:：.]?\s*(.*)', g.strip())
        content = g.strip()
        if m:
            pri = int(m.group(1))
            content = m.group(2)
        
        r_obj = {}
        for cond in re.split(r'[。；;]\s*', content):
            if ':' not in cond and '：' not in cond: continue
            sep = ':' if ':' in cond else '：'
            k, v = cond.split(sep, 1)
            k, v = k.strip(), v.strip()
            
            fk = 'other'
            # [Fix] 加入 '院'，讓學院限制也能被歸類為 dept，這樣前端的 collegeMatch 邏輯才會生效
            if '系' in k or '院' in k: fk='dept'  
            elif '年' in k: fk='grade'
            elif '班' in k: fk='class'
            elif '學號' in k: fk='parity'
            elif '身' in k: fk='identity'
            elif '學制' in k: fk='system'
            elif '指定' in k or '先修' in k: fk='prerequisite'
            elif '人數' in k or '上限' in k: fk='limit'
            
            mode = 'exclude' if ('限非' in v or ('非' in v and fk!='identity')) else 'include'
            val_clean = re.sub(r'限非|非|限', '', v)
            vals = [x.strip() for x in re.split(r'[、,，/或]', val_clean) if x.strip()]
            
            if fk=='grade': vals = [{'一年級':'1','二年級':'2','三年級':'3','四年級':'4'}.get(x,x) for x in vals]
            if fk=='parity':
                r_obj[fk] = {'mode':'include', 'value': 'odd' if '單' in v else ('even' if '雙' in v else 'all')}
                continue
            r_obj[fk] = {'mode': mode, 'values': vals}
        rules.append({'priority': pri, 'rules': r_obj})
    return rules

def main():
    links = get_all_class_links()
    if not links: return
    
    all_data = []
    total = len(links)
    print(f"2. 開始爬取 {total} 個頁面...")
    
    for i, link in enumerate(links):
        if i % 20 == 0: print(f"   進度 {i}/{total}...")
        all_data.extend(scrape_table_page(link))
        time.sleep(random.uniform(0.1, 0.3))
        
    print("3. 資料處理與去重 (保留來源資訊)...")
    unique_map = {}
    
    for r in all_data:
        key = r['課程編號']
        
        # 建立來源標記：這門課是在哪個系、哪個年級被找到的
        source_info = {
            "dept": r['dept_name'],
            "class": r['class_name']
        }

        if key not in unique_map:
            # 第一次發現這門課，初始化 sources 列表
            r['sources'] = [source_info]
            unique_map[key] = r
        else:
            # 這門課已經存在 (例如微積分同時出現在土木系和機械系)
            # 1. 我們把新的來源加入列表
            unique_map[key]['sources'].append(source_info)
            
            # 2. [Optional] 如果原本資料不是通識，但新來源是通識，則更新主顯示資訊 (為了顯示通識類別)
            if "通識" in r['dept_name'] and "通識" not in unique_map[key]['dept_name']:
                # 更新主資料，但保留已經收集到的 sources
                existing_sources = unique_map[key]['sources']
                unique_map[key] = r
                unique_map[key]['sources'] = existing_sources

    df = pd.DataFrame(list(unique_map.values()))
    df["time_parsed"] = df["上課時間/教室"].apply(parse_time_string)
    df["rules_parsed"] = df["分發條件"].apply(parse_criteria_text)
    df["is_required"] = df["必選修"].apply(lambda x: "必" in str(x))
    
    # 輸出
    data = df.to_dict(orient="records")
    with open(OUT_JSON, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print("完成！")

if __name__ == "__main__":
    main()