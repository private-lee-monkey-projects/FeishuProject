import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs
import lark_oapi as lark

# ========= 配置 =========
APP_ID = "cli_a75078bf38db900c"
APP_SECRET = "Qof8bNbAgoDpBEBF6T1DMdKOML8SRFIh"
SPREADSHEET_URL = "https://caka-labs.feishu.cn/base/VmlLbVZgwa1GgXsMCjjcS89MnqL?table=tblOwIVn70DBBl1Z&view=vewC9CjYUc"
TXT_DIR = r"D:\MonkeyProjects\FeishuProject\TestTxt"
os.makedirs(TXT_DIR, exist_ok=True)

# ========= 认证 =========
def get_access_token(app_id, app_secret):
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal/"
    headers = {"Content-Type": "application/json"}
    payload = {"app_id": app_id, "app_secret": app_secret}
    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    data = resp.json()
    if data.get("code") == 0:
        return data["app_access_token"]
    raise Exception("获取访问令牌失败: " + str(data))

# ========= URL 解析 =========
def extract_spreadsheet_id(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    return path.split("/")[-1]

def extract_table_id(url: str) -> str:
    qs = parse_qs(urlparse(url).query)
    return (qs.get("table") or [""])[0]

def extract_view_id(url: str) -> str:
    qs = parse_qs(urlparse(url).query)
    return (qs.get("view") or [""])[0]  # 可能为空

# ========= 获取表数据 =========
def get_spreadsheet_data(spreadsheet_url, client):
    app_token = extract_spreadsheet_id(spreadsheet_url)
    table_id = extract_table_id(spreadsheet_url)
    view_id = extract_view_id(spreadsheet_url)

    print(f"[调试] app_token={app_token}, table_id={table_id}, view_id={view_id or '(未指定)'}")

    all_records, page_token, has_more = [], None, True
    while has_more:
        builder = lark.bitable.v1.ListAppTableRecordRequest.builder() \
            .app_token(app_token) \
            .table_id(table_id) \
            .display_formula_ref(True) \
            .automatic_fields(True) \
            .page_size(500)
        if view_id and view_id.startswith("vew"):
            builder.view_id(view_id)
        if page_token:
            builder.page_token(page_token)

        req = builder.build()
        resp: lark.bitable.v1.ListAppTableRecordResponse = client.bitable.v1.app_table_record.list(req)

        if not resp.success():
            lark.logger.error(
                f"list failed, code: {resp.code}, msg: {resp.msg}, log_id: {resp.get_log_id()}, "
                f"resp: \n{json.dumps(json.loads(resp.raw.content), indent=4, ensure_ascii=False)}"
            )
            return []

        obj = json.loads(lark.JSON.marshal(resp.data, indent=4))
        items = obj.get("items", [])
        all_records.extend([{"record_id": it.get("record_id"), "fields": it.get("fields", {})} for it in items])

        has_more = obj.get("has_more", False)
        page_token = obj.get("page_token")
    return all_records

# ========= 读取 TXT 目录 =========
def read_txt_directory(txt_dir):
    mapping = {}
    if not os.path.isdir(txt_dir):
        print(f"错误：TXT 目录不存在：{txt_dir}")
        return mapping

    for fname in os.listdir(txt_dir):
        fpath = os.path.join(txt_dir, fname)
        if os.path.isfile(fpath) and fname.lower().endswith(".txt"):
            base, _ = os.path.splitext(fname)
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    content = f.read()
            except UnicodeDecodeError:
                with open(fpath, "r", encoding="gbk", errors="ignore") as f:
                    content = f.read()
            mapping[base] = content
    return mapping

# ========= 字段提取（兼容多种返回形态）=========
def extract_text_field(fields: dict, key: str) -> str:
    val = fields.get(key)
    if val is None:
        return ""

    if isinstance(val, str):
        return val.strip()

    if isinstance(val, dict):
        value = val.get("value")
        if isinstance(value, list) and value:
            first = value[0]
            if isinstance(first, dict) and "text" in first:
                return str(first["text"]).strip()
        txt = val.get("text")
        if isinstance(txt, str):
            return txt.strip()
        return ""

    if isinstance(val, list) and val:
        first = val[0]
        if isinstance(first, dict) and "text" in first:
            return str(first["text"]).strip()
        if isinstance(first, str):
            return first.strip()

    return str(val).strip()

# ========= 更新记录 =========
def update_record(record_id, fields_data, access_token, spreadsheet_url):
    app_token = extract_spreadsheet_id(spreadsheet_url)
    table_id = extract_table_id(spreadsheet_url)
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"

    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=utf-8"}
    payload = {"fields": fields_data}

    try:
        resp = requests.put(url, headers=headers, data=json.dumps(payload))
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") == 0:
            print(f"记录 {record_id} 更新成功")
            return True
        print(f"记录 {record_id} 更新失败: {result.get('msg')}")
        return False
    except Exception as e:
        print(f"记录 {record_id} 请求异常: {e}")
        return False

def update_with_retry(record_id, fields_data, access_token, spreadsheet_url, max_retries=3):
    for retry in range(max_retries):
        if update_record(record_id, fields_data, access_token, spreadsheet_url):
            return
        if retry < max_retries - 1:
            print(f"记录 {record_id} 将进行第 {retry + 2} 次重试...")
    print(f"记录 {record_id} 经过 {max_retries} 次重试后仍失败，请检查原因")

# ========= 写入逻辑：按“文本”匹配，把内容写到“计算_json” =========
def write_txts_to_bitable(records, access_token, txt_map, spreadsheet_url):
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = []
        for rec in records:
            record_id = rec.get("record_id")
            fields = rec.get("fields", {})
            text_value = extract_text_field(fields, "文本")
            if not text_value:
                continue

            content = txt_map.get(text_value)
            if content is None:
                continue

            futures.append(
                executor.submit(
                    update_with_retry,
                    record_id,
                    {"计算_json": content},
                    access_token,
                    spreadsheet_url
                )
            )

        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                print(f"线程执行异常: {e}")

# ========= 主流程 =========
def main():
    try:
        access_token = get_access_token(APP_ID, APP_SECRET)
        client = lark.Client.builder().app_id(APP_ID).app_secret(APP_SECRET).log_level(lark.LogLevel.DEBUG).build()

        records = get_spreadsheet_data(SPREADSHEET_URL, client)
        print(f"表格数据已获取，共 {len(records)} 条")

        txt_map = read_txt_directory(TXT_DIR)
        print(f"TXT 文件读取完成，共 {len(txt_map)} 个")

        write_txts_to_bitable(records, access_token, txt_map, SPREADSHEET_URL)
        print("处理完成。")
    except Exception as e:
        print(f"发生错误: {e}")

if __name__ == "__main__":
    main()
