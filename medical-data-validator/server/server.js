const express = require('express');
const multer = require('multer');
const path = require('path');
const cors = require('cors');
const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const { spawn } = require('child_process');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ 
    server,
    // 添加心跳检测
    clientTracking: true,
    perMessageDeflate: false
});

// 配置文件上传
const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        const uploadDir = path.join(__dirname, 'uploads');
        if (!fs.existsSync(uploadDir)) {
            fs.mkdirSync(uploadDir, { recursive: true });
        }
        cb(null, uploadDir);
    },
    filename: function (req, file, cb) {
        cb(null, Date.now() + '-' + file.originalname);
    }
});

const upload = multer({ storage: storage });

// 中间件
app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, '../client/build')));

// WebSocket连接处理
wss.on('connection', (ws) => {
    console.log('Client connected');
    
    // 发送心跳包
    const heartbeat = setInterval(() => {
        if (ws.readyState === WebSocket.OPEN) {
            ws.ping();
        }
    }, 30000); // 每30秒发送一次心跳

    ws.on('pong', () => {
        console.log('Received pong from client');
    });

    ws.on('close', () => {
        console.log('Client disconnected');
        clearInterval(heartbeat);
    });

    ws.on('error', (error) => {
        console.error('WebSocket error:', error);
        clearInterval(heartbeat);
    });
});

// 文件上传处理
app.post('/api/upload', upload.fields([
    { name: 'standard', maxCount: 1 },
    { name: 'validation', maxCount: 1 }
]), async (req, res) => {
    try {
        if (!req.files || !req.files.standard || !req.files.validation) {
            return res.status(400).json({ error: '请上传标准数据和待验证数据文件' });
        }

        const standardFile = req.files.standard[0];
        const validationFile = req.files.validation[0];

        // 验证文件类型
        if (!standardFile.originalname.endsWith('.csv')) {
            return res.status(400).json({ error: '标准数据必须是CSV文件' });
        }

        if (!validationFile.originalname.endsWith('.csv') && !validationFile.originalname.endsWith('.pkl')) {
            return res.status(400).json({ error: '待验证数据必须是CSV或PKL文件' });
        }

        // 创建结果目录
        const resultsDir = path.join(__dirname, 'results');
        if (!fs.existsSync(resultsDir)) {
            fs.mkdirSync(resultsDir, { recursive: true });
        }

        // 生成结果文件名
        const timestamp = Date.now();
        const resultFile = path.join(resultsDir, `result_${timestamp}.csv`);

        // 启动Python处理脚本
        const pythonProcess = spawn('python', [
            'process.py',
            standardFile.path,
            validationFile.path,
            resultFile
        ]);

        let errorOutput = '';
        let stdoutOutput = '';

        // 处理Python脚本的输出
        pythonProcess.stdout.on('data', (data) => {
            const message = data.toString().trim();
            stdoutOutput += message + '\n';
            console.log('Python stdout:', message);
            
            if (message.startsWith('PROGRESS:')) {
                const progress = message.split(':')[1];
                // 广播进度到所有连接的客户端
                wss.clients.forEach((client) => {
                    if (client.readyState === WebSocket.OPEN) {
                        client.send(JSON.stringify({
                            type: 'progress',
                            progress: parseFloat(progress)
                        }));
                    }
                });
            }
        });

        pythonProcess.stderr.on('data', (data) => {
            const error = data.toString().trim();
            errorOutput += error + '\n';
            console.error('Python stderr:', error);
            
            // 发送错误信息到客户端
            wss.clients.forEach((client) => {
                if (client.readyState === WebSocket.OPEN) {
                    client.send(JSON.stringify({
                        type: 'error',
                        message: error
                    }));
                }
            });
        });

        pythonProcess.on('close', (code) => {
            console.log(`Python process exited with code ${code}`);
            console.log('Python stdout:', stdoutOutput);
            console.log('Python stderr:', errorOutput);

            if (code === 0) {
                res.json({
                    success: true,
                    resultFile: `result_${timestamp}.csv`
                });
            } else {
                res.status(500).json({
                    error: '处理文件时出错',
                    details: errorOutput || stdoutOutput
                });
            }
        });

    } catch (error) {
        console.error('Upload error:', error);
        res.status(500).json({ error: '文件上传失败', details: error.message });
    }
});

// 下载结果文件
app.get('/api/download/:filename', (req, res) => {
    const filename = req.params.filename;
    const filePath = path.join(__dirname, 'results', filename);
    
    if (fs.existsSync(filePath)) {
        res.download(filePath);
    } else {
        res.status(404).json({ error: '文件不存在' });
    }
});

// 启动服务器
const PORT = process.env.PORT || 3001;
server.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
}); 