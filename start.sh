#!/bin/bash
cd medical-data-validator
# 启动后端服务器
cd server
node server.js &

# 启动前端开发服务器
cd ../client
npm start 