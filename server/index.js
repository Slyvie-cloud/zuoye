const express = require('express');
const cors = require('cors');
const path = require('path');

const app = express();
const PORT = process.env.PORT || 3001;

// Dify API 配置
const DIFY_API_BASE = 'https://api.dify.ai/v1';
const DIFY_API_KEY = 'app-BFIeuoYR4QiE907erw6g0lVH';

app.use(cors());
app.use(express.json());

// ==================== 静态文件服务（生产环境） ====================
const buildPath = path.join(__dirname, '..', 'build');
app.use(express.static(buildPath));

// ==================== API 代理 ====================

// 代理 Dify Workflow API（非流式 blocking）
app.post('/api/workflow/run', async (req, res) => {
  try {
    const { inputs, user } = req.body;

    console.log(`[Workflow API] Sending request for user: ${user}`);

    const response = await fetch(`${DIFY_API_BASE}/workflows/run`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${DIFY_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        inputs: inputs || {},
        response_mode: 'blocking',
        user: user || 'default-user',
      }),
      signal: AbortSignal.timeout(60000),
    });

    const data = await response.json();

    if (!response.ok) {
      console.error(`[Workflow API] Error: ${response.status}`, data);
      return res.status(response.status).json({
        error: true,
        message: data.message || `Dify API error: ${response.status}`,
        detail: data,
      });
    }

    console.log(`[Workflow API] Success, task_id: ${data.task_id}`);
    res.json(data);
  } catch (error) {
    console.error('[Workflow API] Exception:', error.message);
    res.status(500).json({
      error: true,
      message: error.message,
    });
  }
});

// 代理 Dify Workflow API（流式 streaming SSE）
app.post('/api/workflow/run/stream', async (req, res) => {
  try {
    const { inputs, user } = req.body;

    console.log(`[Stream API] Starting stream for user: ${user}`);

    const response = await fetch(`${DIFY_API_BASE}/workflows/run`, {
      method: 'POST',
      headers: {
        'Authorization': `Bearer ${DIFY_API_KEY}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({
        inputs: inputs || {},
        response_mode: 'streaming',
        user: user || 'default-user',
      }),
      signal: AbortSignal.timeout(120000),
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      console.error(`[Stream API] Error: ${response.status}`, errorData);
      return res.status(response.status).json({
        error: true,
        message: errorData.message || `Dify API error: ${response.status}`,
      });
    }

    // 设置 SSE headers
    res.setHeader('Content-Type', 'text/event-stream');
    res.setHeader('Cache-Control', 'no-cache');
    res.setHeader('Connection', 'keep-alive');
    res.setHeader('X-Accel-Buffering', 'no');
    res.flushHeaders();

    // 转发流式响应
    const reader = response.body.getReader();
    const decoder = new TextDecoder();

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        const chunk = decoder.decode(value, { stream: true });
        res.write(chunk);
      }
    } finally {
      reader.releaseLock();
    }

    res.end();
    console.log(`[Stream API] Stream ended for user: ${user}`);
  } catch (error) {
    console.error('[Stream API] Exception:', error.message);
    if (!res.headersSent) {
      res.status(500).json({ error: true, message: error.message });
    } else {
      res.end();
    }
  }
});

// 健康检查
app.get('/api/health', (req, res) => {
  res.json({ status: 'ok', timestamp: Date.now() });
});

// SPA 路由回退：所有非 API 请求返回 index.html
app.use((req, res) => {
  if (!req.path.startsWith('/api')) {
    res.sendFile(path.join(buildPath, 'index.html'));
  }
});

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Dify Workflow Proxy Server running on http://0.0.0.0:${PORT}`);
  console.log(`Dify API Base: ${DIFY_API_BASE}`);
});
