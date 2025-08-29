# 飞书多维表格批量操作工具

## 项目简介
本项目提供多个功能的Python脚本，用于批量操作飞书多维表格：
1. **UploadPictures.py** - 批量上传图片到飞书表格
2. **UploadText.py** - 批量更新txt文件内容到飞书表格  
3. **ColumnText2Txt_ImageDownload.py** - 下载表格数据和图片

## 主要特性
- ✅ 使用飞书官方SDK，稳定可靠
- ✅ 批量处理，提高效率（减少API调用次数）
- ✅ 自动重试机制，处理网络异常
- ✅ 详细的中文日志输出
- ✅ 支持多种文件格式和编码

## 功能详解

### UploadPictures.py - 图片批量上传
**基本功能需求：**
1. 从本地图片目录读取图片文件（支持jpg、png等格式）
2. 根据图片文件名与表格中"文本"字段进行匹配
3. 将匹配的图片上传到飞书云端获取file_token
4. 批量更新表格记录，将图片写入"图片"列
5. 支持多张图片上传到同一个单元格
6. 使用官方SDK进行文件上传和批量更新操作

**文件命名规则：**
- 支持格式：`1.jpg`、`1_1.png`、`1_2.png` 等
- 匹配逻辑：提取文件名前缀与表格"文本"字段内容匹配

### UploadText.py - 文本内容批量更新  
**基本功能需求：**
1. 从本地txt文件目录读取文本文件内容
2. 根据txt文件名与表格中"文本"字段进行匹配
3. 批量更新表格记录，将txt内容写入"计算_json"列
4. 使用官方SDK进行批量更新操作，提高性能
5. 支持utf-8和gbk编码的文本文件读取
6. 提供错误重试机制和详细的执行日志

### ColumnText2Txt_ImageDownload.py - 数据下载
将指定列数据写入txt文件，并下载指定列的白底图片

## 配置说明

### 1. 飞书应用配置
在使用前，请在脚本中修改以下配置：
```python
APP_ID = "your_app_id"        # 替换为您的飞书应用ID
APP_SECRET = "your_app_secret" # 替换为您的飞书应用密钥
SPREADSHEET_URL = "your_table_url"  # 替换为您的多维表格URL
```

### 2. 本地目录配置
- **UploadPictures.py**: 修改 `Picture_DIR` 为您的图片存储目录
- **UploadText.py**: 修改 `TXT_DIR` 为您的txt文件存储目录

## 使用方法

### 环境准备
```bash
pip install lark_oapi requests
```

### 运行脚本
```bash
# 批量上传图片
python UploadPictures.py

# 批量更新文本内容  
python UploadText.py

# 下载数据和图片
python ColumnText2Txt_ImageDownload.py
```

## 性能优化
- 使用飞书官方SDK的批量更新接口，减少API调用次数
- 分批处理大量数据，避免单次请求过大
- 智能重试机制，处理网络波动
- 合理的延时设置，避免触发接口限流

## 注意事项
1. 请确保飞书应用具有相应的表格读写权限
2. 建议在测试环境先验证功能正常后再在生产环境使用
3. 大批量数据处理时请注意飞书API的调用频率限制
4. 图片文件大小建议控制在合理范围内，避免上传超时

## 更新日志
- **v2.0** - 使用飞书官方SDK，支持批量更新，提升性能和稳定性
- **v1.0** - 基础版本，使用requests库进行API调用