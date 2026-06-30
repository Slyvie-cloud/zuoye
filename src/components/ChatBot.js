import React, { useState, useRef, useEffect, useCallback } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import './ChatBot.css';

const API_BASE = '';
const STORAGE_KEY = 'dify_chatbot_messages';
const MAX_HISTORY = 20;

function ChatBot() {
  const [messages, setMessages] = useState(() => {
    try {
      const saved = localStorage.getItem(STORAGE_KEY);
      return saved ? JSON.parse(saved) : [];
    } catch {
      return [];
    }
  });
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [streamingContent, setStreamingContent] = useState('');
  const [error, setError] = useState(null);
  const [useStream, setUseStream] = useState(true);
  const [copiedId, setCopiedId] = useState(null);
  const messagesEndRef = useRef(null);
  const inputRef = useRef(null);
  const textareaRef = useRef(null);
  const abortControllerRef = useRef(null);

  // 持久化消息
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(messages));
    } catch { /* ignore */ }
  }, [messages]);

  // 自动滚动到底部
  const scrollToBottom = useCallback(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, []);

  useEffect(() => {
    scrollToBottom();
  }, [messages, streamingContent, scrollToBottom]);

  // 自适应 textarea 高度
  useEffect(() => {
    const ta = textareaRef.current;
    if (ta) {
      ta.style.height = 'auto';
      ta.style.height = Math.min(ta.scrollHeight, 120) + 'px';
    }
  }, [input]);

  const generateId = () => `msg_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;

  const addMessage = useCallback((role, content) => {
    const newMsg = { id: generateId(), role, content, time: Date.now() };
    setMessages(prev => {
      const updated = [...prev, newMsg];
      return updated.length > MAX_HISTORY * 2 ? updated.slice(-MAX_HISTORY * 2) : updated;
    });
    return newMsg;
  }, []);

  const buildConversationContext = (currentMessages) => {
    const recent = currentMessages.slice(-6);
    return recent
      .map(m => `${m.role === 'user' ? '用户' : '助手'}: ${m.content}`)
      .join('\n');
  };

  // 格式化时间
  const formatTime = (timestamp) => {
    const d = new Date(timestamp);
    const now = new Date();
    const isToday = d.toDateString() === now.toDateString();
    const time = d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
    if (isToday) return time;
    return `${d.toLocaleDateString('zh-CN', { month: 'short', day: 'numeric' })} ${time}`;
  };

  // 复制消息
  const copyMessage = async (content) => {
    try {
      await navigator.clipboard.writeText(content);
      setCopiedId(content);
      setTimeout(() => setCopiedId(null), 2000);
    } catch {
      // fallback
      const ta = document.createElement('textarea');
      ta.value = content;
      document.body.appendChild(ta);
      ta.select();
      document.execCommand('copy');
      document.body.removeChild(ta);
    }
  };

  // 非流式发送
  const sendBlocking = async (query, context) => {
    const response = await fetch(`${API_BASE}/api/workflow/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        inputs: { query, conversation_history: context },
        user: 'chat-user',
      }),
      signal: AbortSignal.timeout(60000),
    });

    const data = await response.json();

    if (!response.ok || data.error) {
      throw new Error(data.message || '请求失败');
    }

    const outputs = data.data?.outputs || {};
    return outputs.text || outputs.result || outputs.output || JSON.stringify(outputs);
  };

  // 流式发送 (SSE)
  const sendStreaming = async (query, context) => {
    abortControllerRef.current = new AbortController();

    const response = await fetch(`${API_BASE}/api/workflow/run/stream`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        inputs: { query, conversation_history: context },
        user: 'chat-user',
      }),
      signal: abortControllerRef.current.signal,
    });

    if (!response.ok) {
      const data = await response.json().catch(() => ({}));
      throw new Error(data.message || '流式请求失败');
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let fullContent = '';
    let buffer = '';

    try {
      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split('\n');
        buffer = lines.pop() || '';

        for (const line of lines) {
          if (line.startsWith('data: ')) {
            try {
              const eventData = JSON.parse(line.slice(6));

              if (eventData.event === 'workflow_finished') {
                const outputs = eventData.data?.outputs || {};
                fullContent = outputs.text || outputs.result || outputs.output || JSON.stringify(outputs);
                setStreamingContent(fullContent);
              } else if (eventData.event === 'node_finished' && eventData.data?.outputs?.text) {
                fullContent = eventData.data.outputs.text;
                setStreamingContent(fullContent);
              } else if (eventData.event === 'workflow_started') {
                setStreamingContent('正在处理...');
              }
            } catch { /* ignore */ }
          }
        }
      }
    } finally {
      reader.releaseLock();
    }

    return fullContent || '工作流执行完成';
  };

  // 发送消息
  const handleSend = async () => {
    const trimmed = input.trim();
    if (!trimmed || loading) return;

    const context = buildConversationContext(messages);
    addMessage('user', trimmed);
    setInput('');
    setLoading(true);
    setError(null);

    try {
      let outputText;
      if (useStream) {
        setStreamingContent('');
        outputText = await sendStreaming(trimmed, context);
      } else {
        outputText = await sendBlocking(trimmed, context);
      }

      setStreamingContent('');
      addMessage('assistant', outputText);
    } catch (err) {
      if (err.name === 'AbortError') {
        console.log('请求已取消');
      } else {
        console.error('Send error:', err);
        setError(err.message);
        setStreamingContent('');
        addMessage('assistant', `抱歉，请求出错了：${err.message}`);
      }
    } finally {
      setLoading(false);
      abortControllerRef.current = null;
    }
  };

  const handleCancel = () => {
    abortControllerRef.current?.abort();
  };

  const handleClear = () => {
    if (messages.length === 0) return;
    if (window.confirm('确定要清空所有对话记录吗？')) {
      setMessages([]);
      setError(null);
      setStreamingContent('');
      localStorage.removeItem(STORAGE_KEY);
    }
  };

  const handleKeyDown = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      handleSend();
    }
  };

  // 消息气泡组件
  const MessageBubble = ({ msg }) => {
    const isUser = msg.role === 'user';
    return (
      <div className={`message-wrapper ${msg.role}`}>
        <div className={`message-avatar ${msg.role}`}>
          {isUser ? '👤' : '🤖'}
        </div>
        <div className="message-content">
          <div className={`message-bubble ${msg.role}`}>
            {isUser ? (
              msg.content
            ) : (
              <ReactMarkdown
                remarkPlugins={[remarkGfm]}
                components={{
                  code: ({ node, inline, className, children, ...props }) => {
                    const codeStr = String(children).replace(/\n$/, '');
                    if (inline) {
                      return <code className="inline-code" {...props}>{codeStr}</code>;
                    }
                    return (
                      <div className="code-block-wrapper">
                        <div className="code-block-header">
                          <span className="code-lang">{className?.replace('language-', '') || 'code'}</span>
                          <button
                            className="code-copy-btn"
                            onClick={() => copyMessage(codeStr)}
                          >
                            {copiedId === codeStr ? '✓ 已复制' : '📋 复制'}
                          </button>
                        </div>
                        <pre><code className={className} {...props}>{codeStr}</code></pre>
                      </div>
                    );
                  },
                  a: ({ href, children }) => (
                    <a href={href} target="_blank" rel="noopener noreferrer">{children}</a>
                  ),
                  table: ({ children }) => (
                    <div className="table-wrapper"><table>{children}</table></div>
                  ),
                }}
              >
                {msg.content}
              </ReactMarkdown>
            )}
          </div>
          <div className="message-meta">
            <span className="message-time">{formatTime(msg.time)}</span>
            {!isUser && (
              <button
                className="copy-btn"
                onClick={() => copyMessage(msg.content)}
                title="复制"
              >
                {copiedId === msg.content ? '✓' : '📋'}
              </button>
            )}
          </div>
        </div>
      </div>
    );
  };

  return (
    <div className="chatbot-container">
      {/* 头部 */}
      <div className="chatbot-header">
        <div className="header-icon">
          <svg width="28" height="28" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M12 2a10 10 0 1 0 10 10H12V2z"/>
            <path d="M12 2a10 10 0 0 1 10 10h-5a5 5 0 0 0-5-5V2z"/>
            <circle cx="8.5" cy="14.5" r="1.5"/>
            <circle cx="15.5" cy="14.5" r="1.5"/>
            <path d="M8 18c1.5 2 4 2 5.5 2s4-0.5 5.5-2"/>
          </svg>
        </div>
        <div className="header-info">
          <h2>Dify ChatBot</h2>
          <p>Workflow 智能助手</p>
        </div>
        <div className="header-actions">
          <label className="stream-toggle" title="切换流式/非流式响应">
            <input
              type="checkbox"
              checked={useStream}
              onChange={(e) => setUseStream(e.target.checked)}
            />
            <span className="toggle-label">流式</span>
          </label>
          <button className="header-btn" onClick={handleClear} title="清空对话">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <polyline points="3 6 5 6 21 6"/>
              <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"/>
            </svg>
          </button>
        </div>
      </div>

      {/* 消息列表 */}
      <div className="chatbot-messages">
        {messages.length === 0 && !streamingContent && (
          <div className="welcome-message">
            <div className="welcome-icon">
              <svg width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="#667eea" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
              </svg>
            </div>
            <h3>欢迎使用 Dify ChatBot</h3>
            <p>我是基于 Dify Workflow 的智能助手，有什么可以帮你的？</p>
            <div className="welcome-hints">
              <div className="hint-item">
                <kbd>Enter</kbd> 发送消息
              </div>
              <div className="hint-item">
                <kbd>Shift</kbd> + <kbd>Enter</kbd> 换行
              </div>
              <div className="hint-item">
                支持 <strong>Markdown</strong> 格式渲染
              </div>
            </div>
          </div>
        )}
        {messages.map((msg) => (
          <MessageBubble key={msg.id} msg={msg} />
        ))}
        {/* 流式输出 */}
        {streamingContent && (
          <div className="message-wrapper assistant">
            <div className="message-avatar assistant">🤖</div>
            <div className="message-content">
              <div className="message-bubble assistant streaming">
                <ReactMarkdown remarkPlugins={[remarkGfm]}>
                  {streamingContent}
                </ReactMarkdown>
                <span className="cursor">|</span>
              </div>
            </div>
          </div>
        )}
        {/* 加载动画 */}
        {loading && !streamingContent && (
          <div className="message-wrapper assistant">
            <div className="message-avatar assistant">🤖</div>
            <div className="message-content">
              <div className="message-bubble assistant loading">
                <span className="dot">.</span>
                <span className="dot">.</span>
                <span className="dot">.</span>
              </div>
            </div>
          </div>
        )}
        {error && (
          <div className="error-banner">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/>
            </svg>
            <span>{error}</span>
            <button onClick={() => setError(null)}>
              <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2"><line x1="18" y1="6" x2="6" y2="18"/><line x1="6" y1="6" x2="18" y2="18"/></svg>
            </button>
          </div>
        )}
        <div ref={messagesEndRef} />
      </div>

      {/* 输入框 */}
      <div className="chatbot-input">
        <textarea
          ref={(el) => { inputRef.current = el; textareaRef.current = el; }}
          value={input}
          onChange={(e) => setInput(e.target.value)}
          onKeyDown={handleKeyDown}
          placeholder="输入消息，Enter 发送，Shift+Enter 换行..."
          rows={1}
          disabled={loading}
        />
        {loading ? (
          <button className="cancel-button" onClick={handleCancel} title="取消">
            <svg width="16" height="16" viewBox="0 0 24 24" fill="currentColor">
              <rect x="2" y="2" width="20" height="20" rx="2"/>
            </svg>
          </button>
        ) : (
          <button
            className="send-button"
            onClick={handleSend}
            disabled={!input.trim()}
            title="发送 (Enter)"
          >
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <line x1="22" y1="2" x2="11" y2="13"/>
              <polygon points="22 2 15 22 11 13 2 9 22 2"/>
            </svg>
          </button>
        )}
      </div>
    </div>
  );
}

export default ChatBot;
