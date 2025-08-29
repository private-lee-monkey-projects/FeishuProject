import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs
import lark_oapi as lark

# ========= 配置 =========
APP_ID = "cli_a75078bf38db900c"
APP_SECRET = "Qof8bNbAgoDpBEBF6T1DMdKOML8SRFIh"

SPREADSHEET_URL = "https://caka-labs.feishu.cn/base/JkEebVNSMaoVwDsVh8ccGc8rnqc?table=tblCzf1UGL4n1SDp&view=vew4lnxRfz"

# 本地图片目录
PICTURE_DIR = r"D:\MonkeyProjects\FeishuProject\TestPictures"
os.makedirs(PICTURE_DIR, exist_ok=True)

# Bitable 中用来存放图片的字段名（必须是"图片"类型）
IMAGE_FIELD_NAME = "图片"   # ← 改成你实际的图片字段名

# 用于匹配记录的字段名（记录里该字段的值要等于 图片文件名前缀，不含扩展名）
MATCH_FIELD_NAME = "文本"           # ← 仍沿用你之前的"文本"字段做匹配


# ========= 认证 =========
def get_tenant_access_token(app_id: str, app_secret: str) -> str:
    """
    上传文件和更新记录均建议使用 tenant_access_token（企业自建应用）。
    """
    url = "https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal/"
    headers = {"Content-Type": "application/json"}
    payload = {"app_id": app_id, "app_secret": app_secret}
    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    data = resp.json()
    if data.get("code") == 0:
        return data["tenant_access_token"]
    raise Exception("获取租户访问令牌失败: " + str(data))


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


# ========= 读取图片目录（支持1.png、1_1.png等格式）=========
def read_picture_directory_paths(picture_dir):
    """
    返回 { 基础文件名前缀: [图片路径列表] } 的映射
    支持格式：1.png、1_1.png、1_2.png等，会按文件名排序
    """
    mapping = {}
    if not os.path.isdir(picture_dir):
        print(f"错误：图片目录不存在：{picture_dir}")
        return mapping

    for fname in os.listdir(picture_dir):
        fpath = os.path.join(picture_dir, fname)
        if os.path.isfile(fpath) and fname.lower().endswith((".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp")):
            # 解析文件名，支持 1_1.png, 1_2.png, 1_3.png 这种格式
            base = os.path.splitext(fname)[0]  # 去掉扩展名
            if "_" in base:
                # 如果是 1_1 这种格式，提取主键 1
                main_key = base.split("_")[0]
            else:
                # 如果是 1.png 这种格式，直接使用
                main_key = base
            
            if main_key not in mapping:
                mapping[main_key] = []
            mapping[main_key].append(os.path.abspath(fpath))
    
    # 对每个主键的图片列表进行排序，确保 1_1, 1_2, 1_3 的顺序
    for key in mapping:
        mapping[key].sort(key=lambda x: os.path.basename(x))
    
    return mapping


# ========= 兼容提取文本字段（用于匹配记录）=========
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


# ========= 上传图片文件，获取 file_token =========
def upload_image_get_token(file_path: str, tenant_access_token: str, app_token: str) -> str:
    """
    使用 Drive 媒体上传 (upload_all) 获取 file_token。
    多维表图片：parent_type=bitable_file，parent_node=Base 的 app_token。
    """
    url = "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all"
    headers = {"Authorization": f"Bearer {tenant_access_token}"}

    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)
    
    # 根据文件扩展名确定MIME类型
    ext = os.path.splitext(file_name)[1].lower()
    mime_type_map = {
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg', 
        '.png': 'image/png',
        '.gif': 'image/gif',
        '.bmp': 'image/bmp',
        '.webp': 'image/webp'
    }
    mime_type = mime_type_map.get(ext, 'image/jpeg')

    with open(file_path, "rb") as f:
        files = {
            # 文件二进制
            "file": (file_name, f, mime_type),
            # 其余都是普通 form-data 字段
            "file_name": (None, file_name),
            "parent_type": (None, "bitable_file"),   # 关键！！
            "parent_node": (None, app_token),        # 关键！！= Base 的 app_token
            "size": (None, str(file_size)),
        }
        resp = requests.post(url, headers=headers, files=files)
    resp.raise_for_status()
    data = resp.json()
    if data.get("code") == 0 and data.get("data", {}).get("file_token"):
        return data["data"]["file_token"]
    raise Exception(f"上传失败: {data}")


# ========= 更新记录（把多个 file_token 写入图片字段）=========
def update_record_images(record_id, image_field_name, file_tokens, tenant_access_token, spreadsheet_url):
    app_token = extract_spreadsheet_id(spreadsheet_url)
    table_id = extract_table_id(spreadsheet_url)
    url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"

    headers = {"Authorization": f"Bearer {tenant_access_token}", "Content-Type": "application/json; charset=utf-8"}
    
    # 构建图片字段数据（飞书表格中图片字段的格式，支持多张图片）
    image_field_data = []
    for file_token in file_tokens:
        image_field_data.append({
            "file_token": file_token,
            "type": "image"
        })
    
    payload = {
        "fields": {
            # 图片字段值是一个数组，每个元素包含 file_token 和 type
            image_field_name: image_field_data
        }
    }

    resp = requests.put(url, headers=headers, data=json.dumps(payload))
    try:
        resp.raise_for_status()
    except Exception as e:
        raise Exception(f"记录 {record_id} 更新请求失败: {e}, 返回: {resp.text}")

    result = resp.json()
    if result.get("code") == 0:
        print(f"记录 {record_id} 已写入 {len(file_tokens)} 张图片到字段: {image_field_name}")
        return True
    raise Exception(f"记录 {record_id} 更新失败: {result}")


def update_with_retry(record_id, image_field_name, file_tokens, tenant_access_token, spreadsheet_url, max_retries=3):
    for i in range(max_retries):
        try:
            if update_record_images(record_id, image_field_name, file_tokens, tenant_access_token, spreadsheet_url):
                return
        except Exception as e:
            print(f"记录 {record_id} 第 {i+1} 次更新失败: {e}")
        if i < max_retries - 1:
            print(f"记录 {record_id} 将进行第 {i + 2} 次重试...")
    print(f"记录 {record_id} 经过 {max_retries} 次重试后仍失败，请检查原因")


# ========= 主逻辑：按"匹配字段(MATCH_FIELD_NAME)" = 图片文件名前缀 来匹配，再把图片作为附件上传 =========
def write_pictures_to_bitable(records, tenant_access_token, base_to_path_map, spreadsheet_url):
    app_token = extract_spreadsheet_id(spreadsheet_url)  # ← 新增：拿 app_token 传给上传函数
    
    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = []
        for rec in records:
            record_id = rec.get("record_id")
            fields = rec.get("fields", {})
            key = extract_text_field(fields, MATCH_FIELD_NAME)
            if not key:
                continue
            image_paths = base_to_path_map.get(key)
            if not image_paths:
                continue

            def task(record_id=record_id, image_paths=image_paths):
                try:
                    # 上传所有图片并收集file_token
                    file_tokens = []
                    for image_path in image_paths:
                        try:
                            token = upload_image_get_token(image_path, tenant_access_token, app_token)
                            file_tokens.append(token)
                            print(f"图片 {os.path.basename(image_path)} 上传成功，file_token: {token}")
                        except Exception as e:
                            print(f"图片 {os.path.basename(image_path)} 上传失败: {e}")
                            continue
                    
                    if file_tokens:
                        # 批量更新记录，将所有图片写入图片字段
                        update_with_retry(
                            record_id, IMAGE_FIELD_NAME, file_tokens,
                            tenant_access_token, spreadsheet_url
                        )
                    else:
                        print(f"记录 {record_id} 没有成功上传的图片")
                        
                except Exception as e:
                    print(f"处理记录 {record_id} 时发生错误: {e}")

            futures.append(executor.submit(task))

        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                print(f"线程执行异常: {e}")


# ========= 主流程 =========
def main():
    try:
        print("=== 飞书图片批量上传工具启动 ===")
        print(f"图片目录: {PICTURE_DIR}")
        print(f"表格URL: {SPREADSHEET_URL}")
        
        tenant_access_token = get_tenant_access_token(APP_ID, APP_SECRET)
        print("✓ 获取租户访问令牌成功")
        
        client = lark.Client.builder().app_id(APP_ID).app_secret(APP_SECRET).log_level(lark.LogLevel.DEBUG).build()
        print("✓ 飞书SDK客户端初始化成功")

        records = get_spreadsheet_data(SPREADSHEET_URL, client)
        print(f"✓ 表格数据已获取，共 {len(records)} 条记录")

        base_to_path = read_picture_directory_paths(PICTURE_DIR)
        print(f"✓ 图片文件收集完成，共 {len(base_to_path)} 个不同的文件名前缀")
        
        # 显示图片映射信息
        for key, paths in base_to_path.items():
            print(f"  - '{key}': {len(paths)} 张图片")

        print("\n开始批量上传图片并更新表格...")
        write_pictures_to_bitable(records, tenant_access_token, base_to_path, SPREADSHEET_URL)
        print("\n=== 图片上传处理完成 ===")
        
    except Exception as e:
        print(f"\n❌ 程序执行出错: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()
