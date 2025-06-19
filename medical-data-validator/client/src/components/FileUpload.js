import React, { useState, useEffect } from 'react';
import { 
    Box, 
    Button, 
    Typography, 
    LinearProgress, 
    Paper, 
    Grid, 
    Alert, 
    Card, 
    CardContent, 
    Divider,
    Fade,
    Grow,
    Chip,
    Snackbar,
    CircularProgress,
    IconButton,
    Table,
    TableBody,
    TableCell,
    TableContainer,
    TableHead,
    TableRow,
    TablePagination,
    Accordion,
    AccordionSummary,
    AccordionDetails,
    Tooltip
} from '@mui/material';
import CloudUploadIcon from '@mui/icons-material/CloudUpload';
import DownloadIcon from '@mui/icons-material/Download';
import CheckCircleIcon from '@mui/icons-material/CheckCircle';
import ErrorIcon from '@mui/icons-material/Error';
import CloseIcon from '@mui/icons-material/Close';
import InfoIcon from '@mui/icons-material/Info';
import FavoriteIcon from '@mui/icons-material/Favorite';
import LocalHospitalIcon from '@mui/icons-material/LocalHospital';
import ExpandMoreIcon from '@mui/icons-material/ExpandMore';
import VisibilityIcon from '@mui/icons-material/Visibility';
import { styled, alpha } from '@mui/material/styles';

const Input = styled('input')({
    display: 'none',
});

const StyledButton = styled(Button)(({ theme }) => ({
    borderRadius: 8,
    padding: '10px 0',
    transition: 'all 0.3s ease',
    boxShadow: '0 3px 5px 2px rgba(0, 0, 0, 0.05)',
    '&:hover': {
        transform: 'translateY(-2px)',
        boxShadow: '0 6px 10px 2px rgba(25, 118, 210, 0.15)',
    }
}));

const UploadButton = styled(StyledButton)(({ theme }) => ({
    background: `linear-gradient(45deg, #1976d2 30%, #64b5f6 90%)`,
    color: theme.palette.primary.contrastText,
}));

const DownloadButton = styled(StyledButton)(({ theme }) => ({
    background: `linear-gradient(45deg, #43a047 30%, #81c784 90%)`,
    color: theme.palette.success.contrastText,
}));

const StyledPaper = styled(Paper)(({ theme }) => ({
    borderRadius: 16,
    boxShadow: '0 10px 30px rgba(25, 118, 210, 0.08)',
    overflow: 'hidden',
    transition: 'all 0.3s ease',
    background: 'linear-gradient(135deg, #e3f2fd 0%, #ffffff 100%)',
}));

const StyledLinearProgress = styled(LinearProgress)(({ theme }) => ({
    height: 10,
    borderRadius: 5,
    '& .MuiLinearProgress-bar': {
        background: `linear-gradient(90deg, #1976d2, #64b5f6)`,
    },
}));

const FileChip = styled(Chip)(({ theme }) => ({
    margin: theme.spacing(1, 0),
    borderRadius: 16,
    fontWeight: 500,
    boxShadow: '0 2px 5px rgba(25, 118, 210, 0.08)',
}));

const StatusChip = styled(Chip)(({ theme }) => ({
    borderRadius: 16,
    fontWeight: 500,
}));

const HuaxiLogo = () => (
    <Box sx={{ display: 'flex', alignItems: 'center', justifyContent: 'center', mb: 2 }}>
        <LocalHospitalIcon sx={{ color: '#1976d2', fontSize: 48, mr: 2 }} />
        <Typography
            variant="h3"
            sx={{
                fontFamily: 'cursive',
                fontWeight: 900,
                letterSpacing: '0.15em',
                color: '#1976d2',
                textShadow: '2px 2px 8px #bbdefb',
                userSelect: 'none',
            }}
        >
            HUAXI
        </Typography>
    </Box>
);

const FileUpload = () => {
    const [standardFile, setStandardFile] = useState(null);
    const [validationFile, setValidationFile] = useState(null);
    const [uploading, setUploading] = useState(false);
    const [progress, setProgress] = useState(0);
    const [resultFile, setResultFile] = useState(null);
    const [error, setError] = useState(null);
    const [ws, setWs] = useState(null);
    const [wsConnected, setWsConnected] = useState(false);
    const [snackbarOpen, setSnackbarOpen] = useState(false);
    const [snackbarMessage, setSnackbarMessage] = useState('');
    const [snackbarSeverity, setSnackbarSeverity] = useState('info');
    const [resultData, setResultData] = useState([]);
    const [page, setPage] = useState(0);
    const [rowsPerPage, setRowsPerPage] = useState(10);
    const [loading, setLoading] = useState(false);

    useEffect(() => {
        let websocket = null;
        let reconnectAttempts = 0;
        const maxReconnectAttempts = 5;
        const reconnectDelay = 2000; // 2秒

        const connectWebSocket = () => {
            try {
                websocket = new WebSocket('ws://localhost:3001');
                
                websocket.onopen = () => {
                    console.log('WebSocket Connected');
                    setWsConnected(true);
                    setError(null);
                    reconnectAttempts = 0;
                    showSnackbar('服务器连接成功', 'success');
                };

                websocket.onmessage = (event) => {
                    const data = JSON.parse(event.data);
                    if (data.type === 'progress') {
                        setProgress(data.progress);
                    } else if (data.type === 'error') {
                        setError(data.message);
                        showSnackbar(data.message, 'error');
                    }
                };

                websocket.onerror = (error) => {
                    console.error('WebSocket error:', error);
                    setWsConnected(false);
                };

                websocket.onclose = () => {
                    console.log('WebSocket disconnected');
                    setWsConnected(false);
                    
                    // 尝试重连
                    if (reconnectAttempts < maxReconnectAttempts) {
                        reconnectAttempts++;
                        console.log(`Attempting to reconnect (${reconnectAttempts}/${maxReconnectAttempts})...`);
                        setTimeout(connectWebSocket, reconnectDelay);
                    } else {
                        setError('无法连接到服务器，请刷新页面重试');
                        showSnackbar('服务器连接失败，请刷新页面重试', 'error');
                    }
                };

                setWs(websocket);
            } catch (err) {
                console.error('WebSocket connection error:', err);
                setError('WebSocket连接失败，请刷新页面重试');
                showSnackbar('服务器连接失败，请刷新页面重试', 'error');
            }
        };

        connectWebSocket();

        return () => {
            if (websocket) {
                websocket.close();
            }
        };
    }, []);

    const showSnackbar = (message, severity) => {
        setSnackbarMessage(message);
        setSnackbarSeverity(severity);
        setSnackbarOpen(true);
    };

    const handleSnackbarClose = (event, reason) => {
        if (reason === 'clickaway') {
            return;
        }
        setSnackbarOpen(false);
    };

    const handleStandardFileChange = (event) => {
        const file = event.target.files[0];
        if (file && file.name.endsWith('.csv')) {
            setStandardFile(file);
            setError(null);
            showSnackbar(`已选择标准数据: ${file.name}`, 'success');
        } else {
            setError('标准数据必须是CSV文件');
            showSnackbar('标准数据必须是CSV文件', 'error');
        }
    };

    const handleValidationFileChange = (event) => {
        const file = event.target.files[0];
        if (file && (file.name.endsWith('.csv') || file.name.endsWith('.pkl'))) {
            setValidationFile(file);
            setError(null);
            showSnackbar(`已选择待验证数据: ${file.name}`, 'success');
        } else {
            setError('待验证数据必须是CSV或PKL文件');
            showSnackbar('待验证数据必须是CSV或PKL文件', 'error');
        }
    };

    const handleUpload = async () => {
        if (!standardFile || !validationFile) {
            setError('请选择标准数据和待验证数据文件');
            showSnackbar('请选择标准数据和待验证数据文件', 'warning');
            return;
        }

        if (!wsConnected) {
            setError('服务器连接已断开，请刷新页面重试');
            showSnackbar('服务器连接已断开，请刷新页面重试', 'error');
            return;
        }

        setUploading(true);
        setProgress(0);
        setError(null);
        setResultData([]);  // 清空之前的结果
        showSnackbar('开始上传数据...', 'info');

        const formData = new FormData();
        formData.append('standard', standardFile);
        formData.append('validation', validationFile);

        try {
            const response = await fetch('http://localhost:3001/api/upload', {
                method: 'POST',
                body: formData,
            });

            const data = await response.json();

            if (response.ok) {
                setResultFile(data.resultFile);
                showSnackbar('数据验证完成，正在获取结果', 'success');
                
                // 加载结果数据
                fetchResultData(data.resultFile);
            } else {
                setError(data.error + (data.details ? `\n${data.details}` : ''));
                showSnackbar('上传失败: ' + data.error, 'error');
            }
        } catch (err) {
            setError('上传过程中发生错误: ' + err.message);
            showSnackbar('上传错误: ' + err.message, 'error');
            console.error('Upload error:', err);
        } finally {
            setUploading(false);
        }
    };

    const fetchResultData = async (filename) => {
        try {
            setLoading(true);
            const response = await fetch(`http://localhost:3001/api/result/${filename}`);
            
            if (response.ok) {
                const data = await response.json();
                setResultData(data);
                showSnackbar(`成功获取 ${data.length} 条结果数据`, 'success');
            } else {
                showSnackbar('获取结果数据失败', 'error');
            }
        } catch (error) {
            console.error('Error fetching result data:', error);
            showSnackbar('获取结果数据失败: ' + error.message, 'error');
        } finally {
            setLoading(false);
        }
    };

    const handleDownload = () => {
        if (resultFile) {
            window.open(`http://localhost:3001/api/download/${resultFile}`, '_blank');
            showSnackbar('开始下载结果文件', 'info');
        }
    };

    const handleChangePage = (event, newPage) => {
        setPage(newPage);
    };

    const handleChangeRowsPerPage = (event) => {
        setRowsPerPage(parseInt(event.target.value, 10));
        setPage(0);
    };

    return (
        <Box sx={{ minHeight: '100vh', background: 'linear-gradient(135deg, #e3f2fd 0%, #ffffff 100%)', pb: 6 }}>
            <Fade in={true} timeout={800}>
                <StyledPaper elevation={3} sx={{ p: 4, maxWidth: 1200, mx: 'auto', mt: 4 }}>
                    <HuaxiLogo />
                    <Typography 
                        variant="h5" 
                        gutterBottom 
                        align="center"
                        sx={{ mb: 4, fontWeight: 500, color: '#1976d2' }}
                    >
                        数据清洗助手
                    </Typography>
                    <Divider sx={{ mb: 4 }} />
                    <Grid container spacing={3}>
                        <Grid item xs={12} md={6}>
                            <Grow in={true} timeout={1000}>
                                <Card variant="outlined" sx={{ borderRadius: 2, mb: 2 }}>
                                    <CardContent>
                                        <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                                            <InfoIcon fontSize="small" sx={{ verticalAlign: 'middle', mr: 1 }} />
                                            第一步: 选择标准数据
                                        </Typography>
                                        
                                        <Box sx={{ mt: 2 }}>
                                            <label htmlFor="standard-file">
                                                <Input
                                                    accept=".csv"
                                                    id="standard-file"
                                                    type="file"
                                                    onChange={handleStandardFileChange}
                                                    disabled={uploading}
                                                />
                                                <UploadButton
                                                    variant="contained"
                                                    component="span"
                                                    startIcon={<CloudUploadIcon />}
                                                    fullWidth
                                                    disabled={uploading}
                                                >
                                                    选择标准数据文件 (CSV)
                                                </UploadButton>
                                            </label>
                                            {standardFile && (
                                                <Fade in={true}>
                                                    <Box sx={{ mt: 1, textAlign: 'center' }}>
                                                        <FileChip 
                                                            label={standardFile.name}
                                                            color="primary"
                                                            variant="outlined"
                                                            icon={<CheckCircleIcon />}
                                                            onDelete={() => setStandardFile(null)}
                                                        />
                                                    </Box>
                                                </Fade>
                                            )}
                                        </Box>
                                    </CardContent>
                                </Card>
                            </Grow>
                        </Grid>

                        <Grid item xs={12} md={6}>
                            <Grow in={true} timeout={1200}>
                                <Card variant="outlined" sx={{ borderRadius: 2, mb: 2 }}>
                                    <CardContent>
                                        <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                                            <InfoIcon fontSize="small" sx={{ verticalAlign: 'middle', mr: 1 }} />
                                            第二步: 选择待验证数据
                                        </Typography>
                                        
                                        <Box sx={{ mt: 2 }}>
                                            <label htmlFor="validation-file">
                                                <Input
                                                    accept=".csv,.pkl"
                                                    id="validation-file"
                                                    type="file"
                                                    onChange={handleValidationFileChange}
                                                    disabled={uploading}
                                                />
                                                <UploadButton
                                                    variant="contained"
                                                    component="span"
                                                    startIcon={<CloudUploadIcon />}
                                                    fullWidth
                                                    disabled={uploading}
                                                >
                                                    选择待验证数据文件 (CSV)
                                                </UploadButton>
                                            </label>
                                            {validationFile && (
                                                <Fade in={true}>
                                                    <Box sx={{ mt: 1, textAlign: 'center' }}>
                                                        <FileChip 
                                                            label={validationFile.name}
                                                            color="primary"
                                                            variant="outlined"
                                                            icon={<CheckCircleIcon />}
                                                            onDelete={() => setValidationFile(null)}
                                                        />
                                                    </Box>
                                                </Fade>
                                            )}
                                        </Box>
                                    </CardContent>
                                </Card>
                            </Grow>
                        </Grid>

                        <Grid item xs={12}>
                            <Grow in={true} timeout={1400}>
                                <Card variant="outlined" sx={{ borderRadius: 2, mb: 2 }}>
                                    <CardContent>
                                        <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                                            <InfoIcon fontSize="small" sx={{ verticalAlign: 'middle', mr: 1 }} />
                                            第三步: 开始验证
                                        </Typography>
                                        
                                        <Box sx={{ mt: 2 }}>
                                            <StyledButton
                                                variant="contained"
                                                color="primary"
                                                onClick={handleUpload}
                                                disabled={!standardFile || !validationFile || uploading || !wsConnected}
                                                fullWidth
                                                sx={{ mb: 2 }}
                                                startIcon={uploading ? <CircularProgress size={20} color="inherit" /> : null}
                                            >
                                                {uploading ? '处理中...' : '开始验证'}
                                            </StyledButton>
                                        </Box>
                                    </CardContent>
                                </Card>
                            </Grow>
                        </Grid>

                        {uploading && (
                            <Grid item xs={12}>
                                <Fade in={uploading}>
                                    <Card variant="outlined" sx={{ borderRadius: 2, mb: 2 }}>
                                        <CardContent>
                                            <Typography variant="subtitle1" color="text.secondary" gutterBottom>
                                                处理进度
                                            </Typography>
                                            <Box sx={{ width: '100%', mt: 2 }}>
                                                <StyledLinearProgress variant="determinate" value={progress} />
                                                <Typography variant="body2" color="text.secondary" align="center" sx={{ mt: 1 }}>
                                                    {Math.round(progress)}%
                                                </Typography>
                                            </Box>
                                        </CardContent>
                                    </Card>
                                </Fade>
                            </Grid>
                        )}

                        {error && (
                            <Grid item xs={12}>
                                <Grow in={!!error}>
                                    <Alert 
                                        severity="error" 
                                        sx={{ 
                                            whiteSpace: 'pre-wrap',
                                            borderRadius: 2,
                                            boxShadow: '0 2px 8px rgba(0, 0, 0, 0.1)'
                                        }}
                                        action={
                                            <IconButton
                                                aria-label="close"
                                                color="inherit"
                                                size="small"
                                                onClick={() => setError(null)}
                                            >
                                                <CloseIcon fontSize="inherit" />
                                            </IconButton>
                                        }
                                    >
                                        {error}
                                    </Alert>
                                </Grow>
                            </Grid>
                        )}

                        {resultFile && (
                            <Grid item xs={12}>
                                <Grow in={!!resultFile} timeout={500}>
                                    <Card variant="outlined" sx={{ borderRadius: 2, mb: 2, bgcolor: alpha('#4caf50', 0.05) }}>
                                        <CardContent>
                                            <Box sx={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', mb: 2 }}>
                                                <Typography variant="subtitle1" color="success.main" sx={{ display: 'flex', alignItems: 'center' }}>
                                                    <CheckCircleIcon sx={{ mr: 1 }} /> 验证完成
                                                </Typography>
                                                <DownloadButton
                                                    variant="contained"
                                                    color="success"
                                                    startIcon={<DownloadIcon />}
                                                    onClick={handleDownload}
                                                    size="small"
                                                >
                                                    下载结果
                                                </DownloadButton>
                                            </Box>
                                        </CardContent>
                                    </Card>
                                </Grow>
                            </Grid>
                        )}

                        {resultData.length > 0 && (
                            <Grid item xs={12}>
                                <Grow in={resultData.length > 0} timeout={700}>
                                    <Box>
                                        <Typography variant="h6" color="#1976d2" sx={{ mb: 2, display: 'flex', alignItems: 'center' }}>
                                            <FavoriteIcon sx={{ mr: 1, color: '#1976d2' }} /> 验证结果
                                        </Typography>
                                        <Grid container spacing={2}>
                                            {resultData.slice(page * rowsPerPage, page * rowsPerPage + rowsPerPage).map((row, idx) => (
                                                <Grid item xs={12} md={6} lg={4} key={idx}>
                                                    <Card sx={{
                                                        borderLeft: `6px solid ${row.判断结果.includes('不') ? '#f44336' : '#43a047'}`,
                                                        boxShadow: '0 4px 16px rgba(25, 118, 210, 0.08)',
                                                        borderRadius: 3,
                                                        mb: 2,
                                                        background: row.判断结果.includes('不') ? 'linear-gradient(90deg, #fff 60%, #ffebee 100%)' : 'linear-gradient(90deg, #fff 60%, #e8f5e9 100%)',
                                                    }}>
                                                        <CardContent>
                                                            <Box sx={{ display: 'flex', alignItems: 'center', mb: 1 }}>
                                                                {row.判断结果.includes('不') ? (
                                                                    <ErrorIcon color="error" sx={{ mr: 1 }} />
                                                                ) : (
                                                                    <CheckCircleIcon color="success" sx={{ mr: 1 }} />
                                                                )}
                                                                <Typography variant="h6" sx={{ fontWeight: 700, color: row.判断结果.includes('不') ? '#f44336' : '#43a047' }}>
                                                                    {row.字段名}
                                                                </Typography>
                                                            </Box>
                                                            <Typography variant="body2" color="text.secondary" sx={{ mb: 1 }}>
                                                                {row.字段含义}
                                                            </Typography>
                                                            <StatusChip 
                                                                label={row.判断结果}
                                                                color={row.判断结果.includes('不') ? 'error' : 'success'}
                                                                size="small"
                                                                variant="outlined"
                                                                sx={{ mb: 1 }}
                                                            />
                                                            <Accordion sx={{ bgcolor: 'transparent', boxShadow: 'none' }}>
                                                                <AccordionSummary expandIcon={<ExpandMoreIcon />} sx={{ p: 0, minHeight: 'auto' }}>
                                                                    <Typography variant="body2" color="#1976d2">详情</Typography>
                                                                </AccordionSummary>
                                                                <AccordionDetails sx={{ p: 1 }}>
                                                                    {row.问题类别 && row.问题类别 !== '无' && (
                                                                        <Typography variant="body2" color="error.main" gutterBottom>
                                                                            <strong>问题类别:</strong><br />{row.问题类别}
                                                                        </Typography>
                                                                    )}
                                                                    {row.清洗建议 && row.清洗建议 !== '无' && (
                                                                        <Typography variant="body2" color="info.main">
                                                                            <strong>清洗建议:</strong><br />{row.清洗建议}
                                                                        </Typography>
                                                                    )}
                                                                    {(!row.问题类别 || row.问题类别 === '无') && (!row.清洗建议 || row.清洗建议 === '无') && (
                                                                        <Typography variant="body2" color="text.secondary">
                                                                            数据符合要求，无需处理
                                                                        </Typography>
                                                                    )}
                                                                </AccordionDetails>
                                                            </Accordion>
                                                        </CardContent>
                                                    </Card>
                                                </Grid>
                                            ))}
                                        </Grid>
                                    </Box>
                                </Grow>
                            </Grid>
                        )}
                    </Grid>
                </StyledPaper>
            </Fade>
            
            <Snackbar
                open={snackbarOpen}
                autoHideDuration={4000}
                onClose={handleSnackbarClose}
                anchorOrigin={{ vertical: 'bottom', horizontal: 'center' }}
            >
                <Alert 
                    onClose={handleSnackbarClose} 
                    severity={snackbarSeverity} 
                    sx={{ width: '100%', boxShadow: '0 3px 10px rgba(0, 0, 0, 0.2)' }}
                >
                    {snackbarMessage}
                </Alert>
            </Snackbar>
        </Box>
    );
};

export default FileUpload; 