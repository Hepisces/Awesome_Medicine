const express = require('express');
const multer = require('multer');
const path = require('path');
const { spawn } = require('child_process');
const fs = require('fs');
const cors = require('cors');
const WebSocket = require('ws');
const http = require('http');
const csv = require('csv-parser');

const app = express();
const port = 3001;

// 创建HTTP服务器
const server = http.createServer(app);

// 创建WebSocket服务器
const wss = new WebSocket.Server({ server });

// 存储所有连接的WebSocket客户端
const clients = new Set();

// WebSocket连接处理
wss.on('connection', (ws) => {
    console.log('新的WebSocket连接');
    clients.add(ws);
    console.log(`当前连接数: ${clients.size}`);

    ws.on('close', () => {
        clients.delete(ws);
        console.log('WebSocket连接关闭');
        console.log(`当前连接数: ${clients.size}`);
    });
});

// 向所有客户端广播消息
function broadcast(data) {
    console.log(`广播消息: ${JSON.stringify(data)}`);
    let sentCount = 0;
    clients.forEach(client => {
        if (client.readyState === WebSocket.OPEN) {
            client.send(JSON.stringify(data));
            sentCount++;
        }
    });
    console.log(`消息已发送给 ${sentCount}/${clients.size} 个客户端`);
}

// 配置文件上传
const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, path.join(__dirname, 'uploads'));
    },
    filename: function (req, file, cb) {
        cb(null, Date.now() + '-' + file.originalname);
    }
});

const upload = multer({ storage: storage });

app.use(cors());
app.use(express.json());
app.use(express.static(path.join(__dirname, 'static')));

// 确保上传目录存在
const uploadsDir = path.join(__dirname, 'uploads');
if (!fs.existsSync(uploadsDir)){
    fs.mkdirSync(uploadsDir, { recursive: true });
}

// 确保结果目录存在
const resultsDir = path.join(__dirname, 'results');
if (!fs.existsSync(resultsDir)){
    fs.mkdirSync(resultsDir, { recursive: true });
}

// 上传文件处理
app.post('/api/upload', upload.fields([
    { name: 'standard', maxCount: 1 },
    { name: 'validation', maxCount: 1 }
]), (req, res) => {
    if (!req.files || !req.files.standard || !req.files.validation) {
        return res.status(400).json({ success: false, error: '请提供标准数据和待验证数据文件' });
    }

    const standardFile = req.files.standard[0];
    const validationFile = req.files.validation[0];
    const resultFileName = `result-${Date.now()}.csv`;
    const resultFilePath = path.join(__dirname, 'results', resultFileName);

    // 运行Python处理脚本
    const pythonProcess = spawn('python', [
        path.join(__dirname, 'process.py'),
        standardFile.path,
        validationFile.path,
        resultFilePath
    ]);

    pythonProcess.stdout.on('data', (data) => {
        const message = data.toString().trim();
        if (message.startsWith('PROGRESS:')) {
            const progress = parseFloat(message.replace('PROGRESS:', ''));
            broadcast({ type: 'progress', progress });
        } else {
            console.log(`Python stdout: ${message}`);
        }
    });

    pythonProcess.stderr.on('data', (data) => {
        const message = data.toString().trim();
        if (message.startsWith('ERROR:')) {
            const errorMsg = message.replace('ERROR:', '');
            broadcast({ type: 'error', message: errorMsg });
        }
        console.error(`Python stderr: ${message}`);
    });

    pythonProcess.on('close', (code) => {
        console.log(`Python process exited with code ${code}`);
        
        if (code === 0) {
            res.json({ success: true, resultFile: resultFileName });
        } else {
            res.status(500).json({ success: false, error: '处理文件时出错' });
        }
    });
});

// 读取结果文件并发送实时更新
function sendResultsToClients(filePath) {
    const results = [];
    
    fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (data) => {
            results.push(data);
            // 实时发送每一行数据
            broadcast({ type: 'result_update', data });
        })
        .on('end', () => {
            console.log('结果文件读取完成，共有 ' + results.length + ' 行数据');
        })
        .on('error', (err) => {
            console.error('读取结果文件出错:', err);
        });
}

// 下载结果文件
app.get('/api/download/:filename', (req, res) => {
    const filename = req.params.filename;
    const filePath = path.join(__dirname, 'results', filename);
    
    if (fs.existsSync(filePath)) {
        res.download(filePath);
    } else {
        res.status(404).json({ success: false, error: '文件不存在' });
    }
});

// 获取结果数据的API端点
app.get('/api/result/:filename', (req, res) => {
    const filename = req.params.filename;
    const filePath = path.join(__dirname, 'results', filename);
    
    if (fs.existsSync(filePath)) {
        const results = [];
        
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => {
                results.push(data);
            })
            .on('end', () => {
                res.json(results);
            })
            .on('error', (err) => {
                console.error('读取结果文件出错:', err);
                res.status(500).json({ success: false, error: '读取结果文件出错' });
            });
    } else {
        res.status(404).json({ success: false, error: '文件不存在' });
    }
});

// 启动服务器
server.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
}); 