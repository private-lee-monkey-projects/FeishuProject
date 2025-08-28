import os
import requests
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import lark_oapi as lark

# 飞书API的基本配置
APP_ID = "cli_a75078bf38db900c"  # 你的app_id
APP_SECRET = "Qof8bNbAgoDpBEBF6T1DMdKOML8SRFIh"  # 你的app_secret
SPREADSHEET_URL = "https://caka-labs.feishu.cn/base/BD1yb6ZIrar1fdsUmcxczlRCnnb?table=tbl34t2IQhlhre0t&view=vew5cbntWQ"  # 飞书多维表格URL
SAVE_PATH = r"D:/PythonProject/飞书/test"  # 固定的保存路径
os.makedirs(SAVE_PATH, exist_ok=True)


# 获取飞书访问令牌
def get_access_token(app_id, app_secret):
    url = "https://open.feishu.cn/open-apis/auth/v3/app_access_token/internal/"
    headers = {"Content-Type": "application/json"}
    payload = {
        "app_id": app_id,
        "app_secret": app_secret
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    data = response.json()
    if data.get("code") == 0:
        return data.get("app_access_token")
    else:
        raise Exception("获取访问令牌失败: " + data.get("msg"))


# 获取表格数据
def get_spreadsheet_data(spreadsheet_url, client):
    spreadsheet_id = extract_spreadsheet_id(spreadsheet_url)
    table_id = extract_table_id(spreadsheet_url)
    view_id = extract_view_id(spreadsheet_url)

    # 存储所有记录的fields
    all_records = []

    # 初始化分页参数
    page_token = None
    has_more = True

    # 循环请求所有页
    while has_more:
        # 构造请求对象
        request: lark.bitable.v1.ListAppTableRecordRequest = lark.bitable.v1.ListAppTableRecordRequest.builder() \
            .app_token(spreadsheet_id) \
            .table_id(table_id) \
            .view_id(view_id) \
            .display_formula_ref(True) \
            .automatic_fields(True) \
            .page_size(500)

        # 设置分页标记
        if page_token:
            request.page_token(page_token)

        request = request.build()

        # 发起请求
        response: lark.bitable.v1.ListAppTableRecordResponse = client.bitable.v1.app_table_record.list(request)

        # 处理失败返回
        if not response.success():
            lark.logger.error(
                f"client.bitable.v1.app_table_record.list failed, code: {response.code}, "
                f"msg: {response.msg}, log_id: {response.get_log_id()}, "
                f"resp: \n{json.dumps(json.loads(response.raw.content), indent=4, ensure_ascii=False)}"
            )
            return

        # 解析响应数据
        response_data = lark.JSON.marshal(response.data, indent=4)
        json_obj = json.loads(response_data)

        # 提取当前页每条记录的fields和record_id
        current_records = [
            {
                "record_id": item.get("record_id"),
                "fields": item.get("fields", {})
            }
            for item in json_obj.get("items", [])
        ]
        all_records.extend(current_records)

        # 更新分页状态
        has_more = json_obj.get("has_more", False)
        page_token = json_obj.get("page_token")
    return all_records


def extract_spreadsheet_id(url):
    parts = url.split('?')
    return parts[0].split('/')[-1]


def extract_table_id(url):
    table_id = url.split('table=')[-1].split('&')[0]
    return table_id


def extract_view_id(url):
    view_id = url.split('view=')[-1]
    return view_id


# 获取临时下载 URL
def get_temp_download_url(file_tokens, access_token):
    url = "https://open.feishu.cn/open-apis/drive/v1/medias/batch_get_tmp_download_url"
    headers = {
        'Authorization': f'Bearer {access_token}',
        'Content-Type': 'application/json'
    }
    payload = {
        "file_tokens": file_tokens
    }
    response = requests.post(url, headers=headers, data=json.dumps(payload))
    data = response.json()

    if data.get("code") == 0:
        return data['data']['items']
    else:
        raise Exception("获取临时下载 URL 失败: " + data.get("msg"))


# 下载图片并保存（加入重试逻辑）
def download_image(tmp_url, save_path, access_token, retries=5):
    success = False
    for attempt in range(retries):
        try:
            # 下载临时链接
            headers = {"Authorization": f"Bearer {access_token}"}
            response = requests.get(tmp_url, headers=headers)

            if response.status_code == 200:
                # 解析 JSON 响应，获取真实的下载 URL
                url = response.json()['data']['tmp_download_urls'][0]["tmp_download_url"]

                # 通过真实的 URL 下载图片
                img_response = requests.get(url, headers=headers)
                if img_response.status_code == 200:
                    with open(save_path, 'wb') as f:
                        f.write(img_response.content)
                    print(f"图片已保存：{save_path}")
                    success = True
                    break  # 成功下载，跳出重试循环
                else:
                    pass
                    # print(f"图片下载失败: {img_response.status_code}")
            else:
                pass
                # print(f"获取图片下载链接失败: {response.status_code}")

        except requests.exceptions.RequestException as e:
            print(f"请求发生错误: {e}")

        # 如果没有成功下载，则等待 2 秒再重试
        if not success:
            time.sleep(2)

    # 如果重试了 5 次都失败，输出错误信息
    if not success:
        print(f"下载图片 {save_path} 失败，已尝试 {retries} 次。")


# 将指定列数据写入txt文件，并下载白底图片
def write_records_to_txt(records, access_token, name_field="图片名称", txt_field="txt", image_field="示例图"):
    # 使用 ThreadPoolExecutor 来进行多线程下载图片
    with ThreadPoolExecutor(max_workers=5) as executor:  # 控制最大并发数为 5
        future_to_img = {}

        for rec in records:
            fields: dict = rec.get("fields", {})

            # ---------- 取图片名称 ----------
            name_obj = fields.get(name_field)
            if not name_obj:
                continue

            if isinstance(name_obj, dict):
                img_name = name_obj.get("value", [{}])[0].get("text", "")
            elif isinstance(name_obj, list):
                img_name = name_obj[0].get("text", "")
            else:
                img_name = name_obj

            if not img_name:
                continue

            # ---------- 取 TXT 列正文 ----------
            txt_items = fields.get(txt_field, [])
            if not txt_items:
                continue

            if isinstance(txt_items, list):
                txt_content = txt_items[0].get("text", "")
            elif isinstance(txt_items, dict):
                txt_content = txt_items.get("text", "")
            elif isinstance(txt_items, str):
                txt_content = txt_items
            else:
                txt_content = ""

            if not txt_content:
                continue

            # ---------- 写TXT文件 ----------
            txt_filename = os.path.join(SAVE_PATH, f"{img_name}.txt")
            with open(txt_filename, "w", encoding="utf-8") as f:
                f.write(txt_content)
            print(f"已生成：{txt_filename}")

            # ---------- 下载白底图片 ----------
            image_items = fields.get(image_field, [])
            if not image_items:
                continue

            if isinstance(image_items, list):
                tmp_url = image_items[0].get("tmp_url", "")

            if tmp_url:
                image_filename = os.path.join(SAVE_PATH, f"{img_name}.png")  # 白底图片以 .png 格式保存
                # 使用线程池来异步下载图片
                future = executor.submit(download_image, tmp_url, image_filename, access_token)
                future_to_img[future] = img_name  # 追踪任务

        # 等待所有下载任务完成
        for future in as_completed(future_to_img):
            img_name = future_to_img[future]
            try:
                future.result()  # 等待下载结果
            except Exception as e:
                print(f"下载图片 {img_name} 时发生错误: {e}")


# 主函数
def main():
    try:
        # 获取飞书访问令牌
        access_token = get_access_token(APP_ID, APP_SECRET)
        # 创建client
        client = lark.Client.builder() \
            .app_id(APP_ID) \
            .app_secret(APP_SECRET) \
            .log_level(lark.LogLevel.DEBUG) \
            .build()
        # 获取表格数据
        data = get_spreadsheet_data(SPREADSHEET_URL, client)

        # 设置图片名称列和txt内容列的列名
        image_column_name = "图片名称"  # 图片名称列的列名
        txt_column_name = "打标结果加触发词"  # txt内容列的列名
        image_field = "去黄"  # 图片列名
        # 将指定列的数据写入txt文件，并下载白底图片
        write_records_to_txt(data, access_token, image_column_name, txt_column_name, image_field)

    except Exception as e:
        print(f"发生错误: {e}")


if __name__ == "__main__":
    main()
