import pandas as pd
import ast
import re
import json
from datetime import datetime
from typing import Dict, List, Union

"""
# DataFrameChecker 输出格式说明

该类分析DataFrame的列数据，并生成标准化的输出报告。
输出格式为一个DataFrame，包含两列：'column_name'和'info'。

## 'info'列的格式说明

'info'列存储的是一个字典，包含以下结构：

```
{
    "data_type": <数据类型>,
    "value_range": {
        <类型特定的统计信息>
    }
}
```

## 支持的数据类型及其对应的统计信息：

1. **int** - 整数类型
   ```
   {
     "data_type": "int",
     "value_range": {
       "null_count": <空值数量>,
       "null_percentage": <空值百分比>,
       "min": <最小值>,
       "max": <最大值>,
       "mean": <平均值>,
       "median": <中位数>
     }
   }
   ```

2. **float** - 浮点数类型
   ```
   {
     "data_type": "float",
     "value_range": {
       "null_count": <空值数量>,
       "null_percentage": <空值百分比>,
       "min": <最小值>,
       "max": <最大值>,
       "mean": <平均值>,
       "median": <中位数>
     }
   }
   ```

3. **text** - 普通文本类型
   ```
   {
     "data_type": "text",
     "value_range": {
       "null_count": <空值数量>,
       "null_percentage": <空值百分比>,
       "description": "not applicable"  // 文本类型不提供统计信息
     }
   }
   ```

4. **category_text** - 分类文本类型 (唯一值少于5个)
   ```
   {
     "data_type": "category_text",
     "value_range": {
       "null_count": <空值数量>,
       "null_percentage": <空值百分比>,
       "category_values": [<分类值1>, <分类值2>, ...],  // 所有唯一值的列表
       "category_count": <分类数量>  // 唯一值的数量
     }
   }
   ```

5. **category_int** - 分类整数类型 (唯一值少于10个)
   ```
   {
     "data_type": "category_int",
     "value_range": {
       "null_count": <空值数量>,
       "null_percentage": <空值百分比>,
       "category_values": [<分类值1>, <分类值2>, ...],  // 所有唯一整数值的列表
       "category_count": <分类数量>  // 唯一值的数量
     }
   }
   ```

6. **date** - 日期类型
   ```
   {
     "data_type": "date",
     "value_range": {
       "null_count": <空值数量>,
       "null_percentage": <空值百分比>,
       "min_date": <最早日期>,  // 格式: YYYY-MM-DD
       "max_date": <最晚日期>,  // 格式: YYYY-MM-DD
       "date_range_days": <日期范围的天数>,
       "date_format": <日期格式>  // 如: "YYYY-MM-DD", "YYYY-MM-DD HH:MM:SS"等
     }
   }
   ```

7. **category_list[int]** - 整数列表分类类型
   ```
   {
     "data_type": "category_list[int]",
     "value_range": {
       "null_count": <空值数量>,
       "null_percentage": <空值百分比>,
       "category_values": [<分类值1>, <分类值2>, ...],  // 列表中所有出现的整数值
       "category_count": <分类数量>  // 唯一分类值的数量
     }
   }
   ```

8. **list[float]** - 浮点数列表类型
   ```
   {
     "data_type": "list[float]",
     "value_range": {
       "null_count": <空值数量>,
       "null_percentage": <空值百分比>,
       "min": <列表中所有元素的最小值>,
       "max": <列表中所有元素的最大值>,
       "avg_list_length": <列表的平均长度>
     }
   }
   ```

9. **list[mixed]** - 混合类型列表
   ```
   {
     "data_type": "list[mixed]",
     "value_range": {
       "null_count": <空值数量>,
       "null_percentage": <空值百分比>,
       "range": <如果无法解析，则显示"unable to parse">
     }
   }
   ```

10. **unknown** - 未知类型（列全为空值）
    ```
    {
      "data_type": "unknown",
      "value_range": {
        "range": "empty"
      }
    }
    ```
"""


class DataFrameChecker:
    """
    用于检查DataFrame中每列的数据类型和取值范围的工具类
    """
    
    def __init__(self, df: pd.DataFrame):
        """
        初始化DataFrameChecker类
        
        参数:
            df: 要检查的DataFrame
        """
        self.df = df
        self.result_df = pd.DataFrame(columns=['column_name', 'info'])
        # 常见日期格式模式
        self.date_patterns = [
            # ISO格式 (YYYY-MM-DD)
            r'^\d{4}-\d{2}-\d{2}$',
            # 美式日期 (MM/DD/YYYY)
            r'^(\d{1,2})/(\d{1,2})/(\d{4})$',
            # 欧式日期 (DD/MM/YYYY)
            r'^(\d{1,2})\.(\d{1,2})\.(\d{4})$',
            r'^(\d{1,2})-(\d{1,2})-(\d{4})$',
            # 中文日期 (YYYY年MM月DD日)
            r'^(\d{4})年(\d{1,2})月(\d{1,2})日$',
            # 带时间的日期 (YYYY-MM-DD HH:MM:SS)
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$',
            # 带时间的日期 (YYYY/MM/DD HH:MM:SS)
            r'^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$',
            # 时间戳格式
            r'^\d{10}$',  # Unix时间戳 (秒)
            r'^\d{13}$',  # Unix时间戳 (毫秒)
        ]
        
        # 日期格式与其易读表示的映射
        self.date_format_mapping = {
            r'^\d{4}-\d{2}-\d{2}$': 'YYYY-MM-DD',
            r'^(\d{1,2})/(\d{1,2})/(\d{4})$': 'MM/DD/YYYY',
            r'^(\d{1,2})\.(\d{1,2})\.(\d{4})$': 'DD.MM.YYYY',
            r'^(\d{1,2})-(\d{1,2})-(\d{4})$': 'DD-MM-YYYY',
            r'^(\d{4})年(\d{1,2})月(\d{1,2})日$': 'YYYY年MM月DD日',
            r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$': 'YYYY-MM-DD HH:MM:SS',
            r'^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$': 'YYYY/MM/DD HH:MM:SS',
            r'^\d{10}$': 'UNIX时间戳(秒)',
            r'^\d{13}$': 'UNIX时间戳(毫秒)',
        }
        
    def _is_date(self, text: str) -> bool:
        """
        检查字符串是否为日期格式
        
        参数:
            text: 要检查的字符串
            
        返回:
            布尔值，表示是否为日期
        """
        if not isinstance(text, str):
            return False
            
        # 检查是否匹配常见日期格式
        for pattern in self.date_patterns:
            if re.match(pattern, text):
                try:
                    # 尝试解析一些常见格式
                    if re.match(r'^\d{4}-\d{2}-\d{2}$', text):
                        datetime.strptime(text, '%Y-%m-%d')
                        return True
                    elif re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})$', text):
                        datetime.strptime(text, '%m/%d/%Y')
                        return True
                    elif re.match(r'^(\d{1,2})\.(\d{1,2})\.(\d{4})$', text):
                        datetime.strptime(text, '%d.%m.%Y')
                        return True
                    elif re.match(r'^(\d{1,2})-(\d{1,2})-(\d{4})$', text):
                        datetime.strptime(text, '%d-%m-%Y')
                        return True
                    elif re.match(r'^(\d{4})年(\d{1,2})月(\d{1,2})日$', text):
                        # 处理中文日期
                        parts = re.match(r'^(\d{4})年(\d{1,2})月(\d{1,2})日$', text)
                        datetime(int(parts.group(1)), int(parts.group(2)), int(parts.group(3)))
                        return True
                    elif re.match(r'^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$', text):
                        datetime.strptime(text, '%Y-%m-%d %H:%M:%S')
                        return True
                    elif re.match(r'^\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}$', text):
                        datetime.strptime(text, '%Y/%m/%d %H:%M:%S')
                        return True
                    elif re.match(r'^\d{10}$', text):
                        # Unix时间戳 (秒)
                        datetime.fromtimestamp(int(text))
                        return True
                    elif re.match(r'^\d{13}$', text):
                        # Unix时间戳 (毫秒)
                        datetime.fromtimestamp(int(text) / 1000)
                        return True
                except ValueError:
                    # 如果解析失败，则不是有效日期
                    continue
                    
        return False
        
    def _detect_type(self, series: pd.Series) -> str:
        """
        检测Series的数据类型，识别为int、float、text、date或其他类型
        
        参数:
            series: 要检测类型的Series
        
        返回:
            数据类型字符串
        """
        # 移除NaN值以避免影响类型判断
        non_null_series = series.dropna()
        
        if len(non_null_series) == 0:
            return 'unknown'
        
        # 检查是否包含列表形式的字符串 (如 "[2,3,4]")
        if non_null_series.dtype == 'object':
            # 尝试检查第一个非空值是否为列表形式的字符串
            sample_val = non_null_series.iloc[0]
            if isinstance(sample_val, str) and sample_val.startswith('[') and sample_val.endswith(']'):
                try:
                    # 尝试解析字符串为列表
                    parsed_list = ast.literal_eval(sample_val)
                    if isinstance(parsed_list, list) and len(parsed_list) > 0:
                        # 检查列表中的元素类型
                        if all(isinstance(x, int) for x in parsed_list):
                            return 'category_list[int]'
                        elif all(isinstance(x, float) for x in parsed_list):
                            return 'list[float]'
                        else:
                            return 'list[mixed]'
                except (ValueError, SyntaxError):
                    pass  # 不是有效的列表表示
            
            # 检查是否为日期格式
            if all(isinstance(x, str) for x in non_null_series):
                # 抽样检查，如果80%以上是日期格式，则识别为日期类型
                sample_size = min(100, len(non_null_series))  # 最多检查100个样本
                sample = non_null_series.sample(sample_size) if len(non_null_series) > sample_size else non_null_series
                date_count = sum(1 for x in sample if self._is_date(x))
                if date_count / len(sample) >= 0.8:
                    return 'date'
                
                # 检查是否为分类文本（唯一值少于5个）
                if non_null_series.nunique() < 5:
                    return 'category_text'
        
        # 尝试转换为数值类型并检查
        try:
            if pd.to_numeric(non_null_series, errors='coerce').notna().all():
                # 检查是否全为整数
                if (non_null_series.astype(float) % 1 == 0).all():
                    # 检查是否为分类型 (独特值少于10)
                    if non_null_series.nunique() < 10:
                        return 'category_int'
                    return 'int'
                else:
                    return 'float'
        except:
            pass
        
        # 默认为文本类型
        return 'text'
    
    def _get_value_range(self, series: pd.Series, data_type: str) -> Dict:
        """
        获取Series的取值范围
        
        参数:
            series: 要分析的Series
            data_type: 已检测到的数据类型
            
        返回:
            包含取值范围信息的字典
        """
        result = {}
        non_null_series = series.dropna()
        
        if len(non_null_series) == 0:
            return {"range": "empty"}
        
        # 添加空值统计 (适用于所有类型)
        result["null_count"] = int(series.isna().sum())
        result["null_percentage"] = round(float(series.isna().mean() * 100),2)
        
        if data_type == 'int':
            result["min"] = float(non_null_series.min())
            result["max"] = float(non_null_series.max())
            result["mean"] = float(non_null_series.mean())
            result["median"] = float(non_null_series.median())
        elif data_type == 'float':
            result["min"] = float(non_null_series.min())
            result["max"] = float(non_null_series.max())
            result["mean"] = float(non_null_series.mean())
            result["median"] = float(non_null_series.median())
        elif data_type == 'text':
            # text类型的value_range设为不适用
            result["description"] = "not applicable"
        elif data_type == 'category_text':
            # 分类文本，输出所有可能的取值
            result["category_values"] = sorted(list(non_null_series.unique()))
            result["category_count"] = int(non_null_series.nunique())
        elif data_type == 'date':
            # 对于日期类型，提供日期范围
            try:
                # 尝试将所有日期转换为datetime格式
                date_series = pd.to_datetime(non_null_series, errors='coerce')
                valid_dates = date_series.dropna()
                
                if len(valid_dates) > 0:
                    result["min_date"] = valid_dates.min().strftime('%Y-%m-%d')
                    result["max_date"] = valid_dates.max().strftime('%Y-%m-%d')
                    result["date_range_days"] = (valid_dates.max() - valid_dates.min()).days
                    
                    # 识别常见的日期格式并使用易读格式
                    sample = non_null_series.iloc[0]
                    for pattern, readable_format in self.date_format_mapping.items():
                        if re.match(pattern, sample):
                            result["date_format"] = readable_format
                            break
            except:
                result["date_info"] = "unable to parse dates"
        elif data_type == 'category_int':
            # 分类型整数，输出所有可能的取值
            result["category_values"] = sorted(list(map(int, non_null_series.unique())))
            result["category_count"] = int(non_null_series.nunique())
        elif data_type.startswith('category_list'):
            # 处理列表类型的分类变量
            try:
                # 获取所有可能的分类值
                all_categories = set()
                for val in non_null_series:
                    if isinstance(val, str) and val.startswith('[') and val.endswith(']'):
                        parsed = ast.literal_eval(val)
                        if isinstance(parsed, list):
                            all_categories.update(parsed)
                
                if all_categories:
                    result["category_values"] = sorted(list(all_categories))
                    result["category_count"] = len(all_categories)
            except:
                result["category_values"] = "unable to parse"
        elif data_type.startswith('list'):
            # 处理其他列表类型
            try:
                # 解析所有列表
                all_elements = []
                for val in non_null_series:
                    if isinstance(val, str) and val.startswith('[') and val.endswith(']'):
                        parsed = ast.literal_eval(val)
                        if isinstance(parsed, list):
                            all_elements.extend(parsed)
                
                if all_elements:
                    result["min"] = min(all_elements)
                    result["max"] = max(all_elements)
                    result["avg_list_length"] = sum(len(ast.literal_eval(x)) for x in non_null_series) / len(non_null_series)
            except:
                result["range"] = "unable to parse"
        
        return result
    
    def check_all_columns(self) -> pd.DataFrame:
        """
        检查DataFrame中所有列的数据类型和取值范围
        
        返回:
            包含检查结果的DataFrame
        """
        result_data = []
        
        for column in self.df.columns:
            series = self.df[column]
            
            # 检测数据类型
            data_type = self._detect_type(series)
            
            # 获取取值范围
            value_range = self._get_value_range(series, data_type)
            
            # 构建结果
            info = {
                "data_type": data_type,
                "value_range": value_range
            }
            
            # 将结果添加到列表 - 避免双重JSON序列化
            result_data.append({
                "column_name": column,
                "info": info  # 直接保存字典，不转换为JSON字符串
            })
        
        # 创建结果DataFrame
        self.result_df = pd.DataFrame(result_data)
        return self.result_df
    
    def generate_report(self) -> pd.DataFrame:
        """
        生成检查报告
        
        返回:
            n行两列的DataFrame，第一列是列名，第二列是详细信息
        """
        if not len(self.result_df):
            self.check_all_columns()
        
        return self.result_df


def parse_category_values(value_range_str: str) -> Dict:
    """
    解析取值范围字符串中的分类值
    
    支持两种格式：
    1. "[男:1,女:2,未知:9]" 解析为 {"男": 1, "女": 2, "未知": 9}
    2. "[已打印, 暂存, 已访视]" 解析为 {"已打印": "已打印", "暂存": "暂存", "已访视": "已访视"}
    
    参数:
        value_range_str: 取值范围字符串
        
    返回:
        包含分类标签和值的字典
    """
    if pd.isna(value_range_str):
        return {}
    
    # 去除前后的括号
    value_str = value_range_str.strip()
    if value_str.startswith('['):
        value_str = value_str[1:]
    if value_str.endswith(']'):
        value_str = value_str[:-1]
    
    # 解析键值对或纯值列表
    result = {}
    pairs = value_str.split(',')
    
    # 检查是否包含冒号（键值对格式）
    has_colon = any(':' in pair for pair in pairs)
    
    if has_colon:
        # 处理键值对格式 [key:value,key:value]
        for pair in pairs:
            if ':' in pair:
                key, value = pair.split(':', 1)
                key = key.strip()
                value = value.strip()
                
                # 尝试将值转换为数字
                try:
                    if '.' in value:
                        value = float(value)
                    else:
                        value = int(value)
                except ValueError:
                    # 如果无法转换为数字，保留为字符串
                    pass
                
                result[key] = value
    else:
        # 处理纯值列表格式 [value, value, value]
        for value in pairs:
            value = value.strip()
            if value:  # 忽略空值
                # 使用值本身作为键
                result[value] = value
    
    return result


def get_date_format(data_type_str: str) -> str:
    """
    从数据类型字符串中提取日期格式
    
    参数:
        data_type_str: 数据类型字符串
        
    返回:
        日期格式字符串
    """
    # 确保data_type_str是字符串
    if not isinstance(data_type_str, str):
        return 'YYYY-MM-DD'  # 默认格式
    
    data_type_lower = data_type_str.lower()
    
    # 检查是否包含具体的日期格式描述
    if "yyyy-mm-dd hh:mm:ss" in data_type_lower or "datetime" in data_type_lower:
        return 'YYYY-MM-DD HH:MM:SS'
    elif "yyyy-mm-dd" in data_type_lower:
        return 'YYYY-MM-DD'
    elif "hh:mm:ss" in data_type_lower or "time" in data_type_lower:
        return 'HH:MM:SS'
    
    # 检查指标类型中的关键词
    if "timestamp" in data_type_lower or "datetime" in data_type_lower:
        return 'YYYY-MM-DD HH:MM:SS'
    elif "date" in data_type_lower:
        return 'YYYY-MM-DD'
    elif "time" in data_type_lower:
        return 'HH:MM:SS'
    
    # 默认返回标准日期格式
    return 'YYYY-MM-DD'


def process_metric_definition(metrics_df: pd.DataFrame) -> pd.DataFrame:
    """
    处理指标定义表格，将其转换为标准格式
    
    表格应包含以下列:
    - 指标编码 (可选)
    - 指标版本 (可选)
    - 指标名 (必需)
    - 字段名 (必需)
    - 统一指标类型 (必需)
    - 取值范围 (可选)
    
    参数:
        metrics_df: 包含指标定义的DataFrame
        
    返回:
        标准格式的指标定义DataFrame，包含列名和类型信息
    """
    # 确保必要的列存在
    required_cols = ['字段名', '统一指标类型']
    for col in required_cols:
        if col not in metrics_df.columns:
            raise ValueError(f"缺少必要的列: {col}")
    
    result_data = []
    
    for _, row in metrics_df.iterrows():
        field_name = row['字段名']
        data_type_raw = row['统一指标类型']
        value_range = row.get('取值范围', None)
        
        # 初始化信息字典，与DataFrameChecker格式保持一致
        info = {
            "real_name": row.get('指标名', ''),  # 指标含义改为real_name
            "data_type": "",
            "value_range": {}
        }
        
        # 根据统一指标类型和取值范围确定数据类型
        if pd.isna(data_type_raw):
            info["data_type"] = "unknown"
            info["value_range"] = {"range": "empty"}
        else:
            # 判断是否是分类变量（根据取值范围是否非空）
            is_category = pd.notna(value_range) and value_range and (
                ('[' in str(value_range) and ']' in str(value_range)) or 
                (',' in str(value_range))
            )
            
            # 直接使用统一指标类型作为data_type，仅在需要时添加category前缀
            data_type = str(data_type_raw).strip()
            data_type_lower = data_type.lower()
            
            # 如果是分类型且原类型不是以list或category开头
            if is_category and not data_type_lower.startswith("list") and not data_type_lower.startswith("category"):
                # 特殊处理几种基础类型
                if data_type_lower in ['int', 'integer', 'bigint']:
                    info["data_type"] = "category_int"
                elif data_type_lower in ['text', 'string', 'varchar', 'char']:
                    info["data_type"] = "category_text"
                else:
                    # 其他类型添加category_前缀
                    info["data_type"] = f"category_{data_type}"
            else:
                # 直接使用原类型
                info["data_type"] = data_type
            
            # 设置value_range
            if info["data_type"] == "unknown":
                info["value_range"] = {"range": "empty"}
            elif info["data_type"].lower() in ['int', 'integer', 'bigint']:
                info["value_range"] = {
                    "null_count": 0,
                    "null_percentage": 0.0,
                    "min": 0.0,
                    "max": 0.0,
                    "mean": 0.0,
                    "median": 0.0
                }
            elif info["data_type"].lower() in ['float', 'double', 'decimal', 'numeric']:
                info["value_range"] = {
                    "null_count": 0,
                    "null_percentage": 0.0,
                    "min": 0.0,
                    "max": 0.0,
                    "mean": 0.0,
                    "median": 0.0
                }
            elif info["data_type"].lower() in ['text', 'string', 'varchar', 'char'] and not info["data_type"].startswith("category_"):
                info["value_range"] = {
                    "null_count": 0,
                    "null_percentage": 0.0,
                    "description": "not applicable"
                }
            elif info["data_type"].startswith("category_"):
                try:
                    # 解析分类值
                    parsed_categories = parse_category_values(value_range)
                    
                    if info["data_type"] == "category_int" and any(isinstance(v, int) for v in parsed_categories.values()):
                        # 对于category_int，使用数值作为category_values
                        category_values = sorted(list(v for v in parsed_categories.values() if isinstance(v, int)))
                    else:
                        # 对于其他分类类型，使用键作为category_values
                        category_values = sorted(list(parsed_categories.keys()))
                    
                    info["value_range"] = {
                        "null_count": 0,
                        "null_percentage": 0.0,
                        "category_values": category_values,
                        "category_count": len(category_values)
                    }
                except Exception:
                    # 解析失败时，提供空的分类信息
                    info["value_range"] = {
                        "null_count": 0,
                        "null_percentage": 0.0,
                        "category_values": [],
                        "category_count": 0
                    }
            elif "date" in info["data_type"].lower() or "time" in info["data_type"].lower():
                # 日期类型
                date_format = get_date_format(data_type_raw)
                
                info["value_range"] = {
                    "null_count": 0,
                    "null_percentage": 0.0,
                    "date_format": date_format,
                    "min_date": "",
                    "max_date": "",
                    "date_range_days": 0
                }
            elif info["data_type"].lower().startswith("list[int]") or info["data_type"].lower().startswith("array[int]"):
                try:
                    parsed_categories = parse_category_values(value_range)
                    category_values = []
                    if parsed_categories:
                        if any(isinstance(v, int) for v in parsed_categories.values()):
                            category_values = sorted(list(v for v in parsed_categories.values() if isinstance(v, int)))
                        else:
                            category_values = sorted(list(parsed_categories.keys()))
                    
                    info["value_range"] = {
                        "null_count": 0,
                        "null_percentage": 0.0,
                        "category_values": category_values,
                        "category_count": len(category_values)
                    }
                except Exception:
                    info["value_range"] = {
                        "null_count": 0,
                        "null_percentage": 0.0,
                        "category_values": [],
                        "category_count": 0
                    }
            elif info["data_type"].lower().startswith("list[float]") or info["data_type"].lower().startswith("array[float]"):
                info["value_range"] = {
                    "null_count": 0,
                    "null_percentage": 0.0,
                    "min": 0.0,
                    "max": 0.0,
                    "avg_list_length": 0.0
                }
            elif info["data_type"].lower().startswith("list") or info["data_type"].lower().startswith("array"):
                info["value_range"] = {
                    "null_count": 0,
                    "null_percentage": 0.0,
                    "range": "unable to parse"
                }
            else:
                # 其他未知类型，提供基本的value_range
                info["value_range"] = {
                    "null_count": 0,
                    "null_percentage": 0.0,
                    "description": "not applicable"
                }
        
        # 将结果添加到列表
        result_data.append({
            "column_name": field_name,
            "info": info
        })
    
    return pd.DataFrame(result_data)






