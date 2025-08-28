# 上传图片到飞书表格的指定列

import os
import json
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse, parse_qs
import lark_oapi as lark
import time

# 飞书API的基本配置
APP_ID = "cli_a75078bf38db900c"
APP_SECRET = "Qof8bNbAgoDpBEBF6T1DMdKOML8SRFIh"
SPREADSHEET_URL = "https://caka-labs.feishu.cn/base/AJC8bGJrnalMKwsQyIBcRPfAnld?table=tbl1P0OzYGDy6Iea&view=vewDcfKbcH"
Picture_DIR = r"D:\PycharmProjects\FeishuProject\TestPictures"
os.makedirs(Picture_DIR, exist_ok=True)

# 获取飞书访问令牌
def get_access_token(app_id, app_secret):
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal/"
    headers = {"Content-Type": "application/json"}
    payload = {"app_id": app_id, "app_secret": app_secret}
    resp = requests.post(url, headers=headers, data=json.dumps(payload))
    data = resp.json()
    if data.get("code") == 0:
        return data["app_access_token"]
    raise Exception("获取访问令牌失败: " + str(data))

# URL 解析
def extract_spreadsheet_id(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    return path.split("/")[-1]

def extract_table_id(url: str) -> str:
    qs = parse_qs(urlparse(url).query)
    return (qs.get("table") or [""])[0]

def extract_view_id(url: str) -> str:
    qs = parse_qs(urlparse(url).query)
    return (qs.get("view") or [""])[0]  # 可能为空

# 获取多维表格数据
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

# 读取图片文件夹
def read_picture_directory(picture_dir):
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
            mapping[main_key].append(fpath)
    
    # 对每个主键的图片列表进行排序，确保 1_1, 1_2, 1_3 的顺序
    for key in mapping:
        mapping[key].sort(key=lambda x: os.path.basename(x))
    
    return mapping

# 上传图片 
def update_picture(record_id, fields_data, access_token, spreadsheet_url):
    app_token = extract_spreadsheet_id(spreadsheet_url)
    table_id = extract_table_id(spreadsheet_url)
    view_id = extract_view_id(spreadsheet_url)

    print(f"[调试] app_token={app_token}, table_id={table_id}, view_id={view_id or '(未指定)'}")    

    try:
        # 获取图片路径列表
        image_paths = fields_data.get("图片路径")
        if not image_paths:
            print(f"没有图片路径")
            return False
        
        # 如果只有一张图片，转换为列表格式
        if isinstance(image_paths, str):
            image_paths = [image_paths]
        
        uploaded_images = []
        
        # 逐个上传图片
        for image_path in image_paths:
            if not os.path.exists(image_path):
                print(f"图片文件不存在: {image_path}")
                continue
                
            # 第一步：上传图片文件到飞书
            upload_url = "https://open.feishu.cn/open-apis/drive/v1/medias/upload_all"
            headers = {"Authorization": f"Bearer {access_token}"}
            
            with open(image_path, 'rb') as f:
                files = {'file': (os.path.basename(image_path), f, 'image/png')}
                data = {'type': 'image'}
                
                # 设置较长的超时时间，处理大文件上传
                upload_resp = requests.post(upload_url, headers=headers, files=files, data=data, timeout=60)
                upload_resp.raise_for_status()
                upload_result = upload_resp.json()
                
                if upload_result.get("code") != 0:
                    print(f"图片 {image_path} 上传失败: {upload_result.get('msg')}")
                    continue
                    
                file_token = upload_result.get("data", {}).get("file_token")
                if not file_token:
                    print(f"图片 {image_path} 未获取到文件token")
                    continue
                
                # 添加到已上传图片列表
                uploaded_images.append({
                    "file_token": file_token,
                    "name": os.path.basename(image_path),
                    "type": "image"
                })
                print(f"图片 {image_path} 上传成功")
                
                # 添加延迟，避免网络拥塞
                time.sleep(1)
        
        if not uploaded_images:
            print(f"没有图片上传成功")
            return False
        
        # 第二步：更新表格记录，将所有图片写入"图片"列
        update_url = f"https://open.feishu.cn/open-apis/bitable/v1/apps/{app_token}/tables/{table_id}/records/{record_id}"
        
        # 构建图片字段数据（飞书表格中图片字段的格式，支持多张图片）
        image_field_data = {
            "图片": uploaded_images
        }
        
        update_headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json; charset=utf-8"}
        update_payload = {"fields": image_field_data}
        
        # 设置超时时间
        update_resp = requests.put(update_url, headers=update_headers, data=json.dumps(update_payload), timeout=30)
        update_resp.raise_for_status()
        update_result = update_resp.json()
        
        if update_result.get("code") == 0:
            print(f"记录 {record_id} 图片更新成功，共 {len(uploaded_images)} 张图片")
            return True
        else:
            print(f"记录 {record_id} 图片更新失败: {update_result.get('msg')}")
            return False
            
    except Exception as e:
        print(f"记录 {record_id} 图片上传异常: {e}")
        return False

def update_picture_with_retry(record_id, fields_data, access_token, spreadsheet_url, max_retries=3):
    for retry in range(max_retries):
        if update_picture(record_id, fields_data, access_token, spreadsheet_url):
            return
        if retry < max_retries - 1:
            print(f"记录 {record_id} 图片将进行第 {retry + 2} 次重试...")
    print(f"记录 {record_id} 图片经过 {max_retries} 次重试后仍失败，请检查原因")

# 写入逻辑：按"文本"匹配，把图片上传到"图片"列
def write_pictures_to_bitable(records, access_token, picture_map, spreadsheet_url):
    with ThreadPoolExecutor(max_workers=10) as executor:  # 参考UploadText.py的成功设置
        futures = []
        for rec in records:
            record_id = rec.get("record_id")
            fields = rec.get("fields", {})
            text_value = extract_text_field(fields, "文本")
            if not text_value:
                continue

            # 对于每个文本值，尝试从图片映射中获取所有图片路径
            picture_paths = picture_map.get(text_value)
            if not picture_paths:
                continue

            # 一次性上传该文本对应的所有图片到同一个单元格
            futures.append(
                executor.submit(
                    update_picture_with_retry,
                    record_id,
                    {"图片路径": picture_paths},
                    access_token,
                    spreadsheet_url
                )
            )

        for fut in as_completed(futures):
            try:
                fut.result()
            except Exception as e:
                print(f"线程执行异常: {e}")

# 提取字段
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

# 主流程
def main():
    try:
        access_token = get_access_token(APP_ID, APP_SECRET)
        client = lark.Client.builder().app_id(APP_ID).app_secret(APP_SECRET).log_level(lark.LogLevel.DEBUG).build()

        records = get_spreadsheet_data(SPREADSHEET_URL, client)
        print(records)
        print(f"表格数据已获取，共 {len(records)} 条")

        picture_map = read_picture_directory(Picture_DIR)
        print(picture_map)
        print(f"图片文件读取完成，共 {len(picture_map)} 个")

        write_pictures_to_bitable(records, access_token, picture_map, SPREADSHEET_URL)
        print("图片上传处理完成。")
    except Exception as e:
        print(f"发生错误: {e}")

if __name__ == "__main__":
    main()
    