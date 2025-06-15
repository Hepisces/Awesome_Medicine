import sys
import pandas as pd
import json
import pickle
from itertools import product
import requests
from check import DataFrameChecker
import traceback
import numpy as np
from joblib import load as joblib_load

def print_progress(progress):
    """输出进度信息到标准输出"""
    print(f"PROGRESS:{progress}", flush=True)

def print_error(error_msg):
    """输出错误信息到标准错误"""
    print(f"ERROR:{error_msg}", file=sys.stderr, flush=True)

def print_info(info_msg):
    """输出普通信息到标准输出"""
    print(f"INFO:{info_msg}", flush=True)

def load_data(file_path):
    """加载数据文件"""
    try:
        if file_path.endswith('.csv'):
            return pd.read_csv(file_path, low_memory=False)  # 添加low_memory=False来避免警告
        elif file_path.endswith('.pkl'):
            try:
                # 首先尝试使用joblib加载
                print_info("尝试使用joblib加载文件...")
                return joblib_load(file_path)
            except Exception as e1:
                print_error(f"joblib加载失败: {str(e1)}")
                try:
                    # 如果joblib失败，尝试使用pandas加载
                    print_info("尝试使用pandas加载文件...")
                    return pd.read_pickle(file_path)
                except Exception as e2:
                    print_error(f"pandas加载失败: {str(e2)}")
                    try:
                        # 最后尝试使用pickle加载
                        print_info("尝试使用pickle加载文件...")
                        with open(file_path, 'rb') as f:
                            return pickle.load(f)
                    except Exception as e3:
                        print_error(f"pickle加载失败: {str(e3)}")
                        raise
        else:
            raise ValueError(f"不支持的文件格式: {file_path}")
    except Exception as e:
        print_error(f"加载文件 {file_path} 时出错: {str(e)}")
        raise

def process_data(standard_file, validation_file, output_file):
    """处理数据并输出结果"""
    try:
        print_info("开始加载标准数据...")
        try:
            standard_df = pd.read_csv(standard_file)
            if 'column_name' not in standard_df.columns or 'info' not in standard_df.columns:
                raise ValueError("标准数据文件格式错误：必须包含 'column_name' 和 'info' 列")
            print_info(f"标准数据加载完成，共 {len(standard_df)} 行")
        except Exception as e:
            print_error(f"加载标准数据失败: {str(e)}")
            raise

        print_info("开始加载待验证数据...")
        try:
            input_df = load_data(validation_file)
            print_info(f"待验证数据加载完成，共 {len(input_df)} 行")
        except Exception as e:
            print_error(f"加载待验证数据失败: {str(e)}")
            raise

        # API URL
        url = "https://api.coze.cn/v1/workflow/run"
        
        # Headers
        headers = {
            "Authorization": "Bearer pat_nyMlcg3dx8QHpRX1Ym09eaWuhnCWj09gkVCBJw2mvvmLKPaLjWGs71VQsDZp2Lh3",
            "Content-Type": "application/json",
        }

        # 初始化结果列表
        results = []

        print_info("初始化检查器...")
        try:
            checker = DataFrameChecker(input_df)
        except Exception as e:
            print_error(f"初始化检查器失败: {str(e)}")
            raise

        print_info("生成报告...")
        try:
            data_df = checker.generate_report()
            if data_df.empty:
                raise ValueError("生成的报告为空")
            print_info(f"报告生成完成，共 {len(data_df)} 列")
        except Exception as e:
            print_error(f"生成报告失败: {str(e)}")
            raise

        total_columns = len(data_df)
        processed_columns = 0

        # 处理前3列数据
        for _, data in data_df[:3].iterrows():
            try:
                column_name = data['column_name']
                info = data['info']
                
                print_info(f"处理列: {column_name}")
                
                # 直接使用info，不需要replace
                distribution = info
                
                # 获取标准数据中的对应列信息
                try:
                    sd_info = standard_df[standard_df['column_name'] == column_name]['info'].values[0]
                    sd_distribution = sd_info
                except Exception as e:
                    print_error(f"获取列 {column_name} 的标准数据失败: {str(e)}")
                    continue

                data = {
                    "workflow_id": "7512734874189987881",
                    "parameters": {
                        "input": {
                            "name": column_name,
                            "distribution": distribution,
                            "sd_distribution": sd_distribution,
                        }
                    },
                }

                print_info(f"发送API请求: {column_name}")
                try:
                    response = requests.post(url, headers=headers, data=json.dumps(data))
                    response.raise_for_status()
                    
                    # 保持原有的响应解析方式
                    response_data = json.loads(json.loads(response.json()['data'])['data'])
                    
                    # 将结果添加到列表
                    result = {
                        '字段名': column_name,
                        '字段含义': json.loads(sd_info.replace("'", '"')).get('real_name', ''),
                        '判断结果': response_data.get('判断结果', '未知'),
                        '问题类别': '\n'.join([f"{i+1}. {item}" for i, item in enumerate(response_data.get('问题类别', ['无']))]) if '不' in response_data.get('判断结果', '') else '无',
                        '清洗建议': '\n'.join([f"{i+1}. {item}" for i, item in enumerate(response_data.get('清洗建议', ['无']))])if '不' in response_data.get('判断结果', '') else '无'
                    }
                    results.append(result)
                    
                except Exception as e:
                    print_error(f"API请求失败: {str(e)}")
                    print_error(f"错误详情: {traceback.format_exc()}")
                    continue

                processed_columns += 1
                progress = (processed_columns / total_columns) * 100
                print_progress(progress)

            except Exception as e:
                print_error(f"处理列 {column_name} 时出错: {str(e)}")
                print_error(traceback.format_exc())
                continue

        if not results:
            raise ValueError("没有成功处理任何列")

        print_info("保存结果...")
        try:
            # 从列表创建DataFrame
            result_df = pd.DataFrame(results)
            result_df.to_csv(output_file, index=False)
            print_info("结果保存完成")
        except Exception as e:
            print_error(f"保存结果失败: {str(e)}")
            raise

        return True

    except Exception as e:
        print_error(f"处理数据时出错: {str(e)}")
        print_error(traceback.format_exc())
        return False

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print_error("Usage: python process.py <standard_file> <validation_file> <output_file>")
        sys.exit(1)
    
    standard_file = sys.argv[1]
    validation_file = sys.argv[2]
    output_file = sys.argv[3]
    
    success = process_data(standard_file, validation_file, output_file)
    sys.exit(0 if success else 1) 