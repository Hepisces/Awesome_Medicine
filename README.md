# 医疗数据验证系统（Medical Data Validator）

## 项目简介

本系统为医疗数据标准化与验证的专业工具，支持上传标准数据（CSV）和待验证数据（CSV/PKL），自动对比并生成详细的验证报告。前端采用 React + MUI，后端基于 Node.js + Python，界面美观、交互友好，适合医疗数据治理、质控、科研等场景。

## 主要功能

- 支持标准数据与待验证数据的上传（CSV/PKL）
- 实时进度反馈，错误友好提示
- 验证结果网页端卡片式展示，支持一键下载
- 医疗蓝风格 UI，顶部 HUAXI 艺术字 LOGO
- 支持多用户并发，适配主流浏览器

## 目录结构

```
medical-data-validator/
|     ├── client/         # 前端React项目
|     ├── server/         # 后端Node.js+Python服务
|     ├── node_modules/   # 依赖
|     ├── package.json    # Node依赖配置
|     ├── start.sh        # 一键启动脚本
├── README.md       # 项目说明
└── requirements.txt# Python依赖
```

## 环境依赖

- Node.js >= 16.x
- Python >= 3.12
- 推荐操作系统：macOS/Linux/WSL2/Windows

## 安装与启动

1. **克隆仓库**
   ```bash
   git clone git@github.com:Hepisces/Awesome_Medicine.git
   cd medical-data-validator
   ```
2. **安装 Node 依赖**
   ```bash
   npm install
   cd client && npm install && cd ..
   ```
3. **安装 Python 依赖**
   ```bash
   pip install -r requirements.txt
   ```
4. **配置 API 密钥**
   首先在[Coze](https://coze.cn)注册账号，并创建 API 密钥，然后将其写入`api_key.txt`文件中
   ```bash
   cd medical-data-validator/server
   echo your_api_key > api_key.txt
   ```
5. **一键启动**

   ```bash
   ./start.sh
   ```

   > 首次运行请确保`start.sh`有可执行权限：`chmod +x start.sh`

6. **访问系统**
   打开浏览器访问 [http://localhost:3001](http://localhost:3001)

## 使用说明

- 上传标准数据（CSV）和待验证数据（CSV）
- 点击"开始验证"，等待进度条完成
- 验证结果将以卡片形式展示，可直接网页查看或下载 CSV 报告
- 支持多用户并发操作

## 常见问题

- **端口冲突**：如 3001 端口被占用，请在`server/app.js`中修改端口
- **依赖问题**：请严格按照`requirements.txt`和`package.json`安装依赖
- **前端样式异常**：请使用最新版Chrome/Edge/Firefox
- **Python脚本报错**：请确保Python版本和依赖一致，且数据文件格式正确
- **API 密钥问题**：请确保 API 密钥正确，且有足够的权限

## 展示视频

我们展示了一个简单的demo以说明在打开网页后的使用方式以供参考
![gif](demo.gif)

[视频连接](https://github.com/user-attachments/assets/23508d4a-fc01-4be9-84f3-b0075436fe5a)

## 数据获取

基于保密需求，我们不会上传任何源数据到仓库，如果您真的需要进行实验，可以邮件联系仓库所有者并说明理由，我们会在评估后给予您回复
