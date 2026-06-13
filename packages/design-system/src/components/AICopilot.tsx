import { useState, useRef, useEffect } from "react";

interface Message {
  id: string;
  role: "user" | "assistant";
  content: string;
  timestamp: Date;
  actions?: Array<{ label: string; action: string }>;
  status?: "thinking" | "executing" | "complete" | "error";
}

const SUGGESTIONS = [
  "Move Snowflake sales data to MongoDB",
  "Sync Oracle customers to PostgreSQL",
  "Map employee records automatically",
  "Validate migration risks for HR data",
  "Schedule nightly sync from S3 to BigQuery",
];

interface AICopilotProps {
  messages: Message[];
  onSend: (message: string) => void;
  onAction?: (action: string) => void;
  isThinking?: boolean;
}

function TypingIndicator() {
  return (
    <div className="dt-typing-indicator">
      <span />
      <span />
      <span />
    </div>
  );
}

function MessageBubble({ message, onAction }: { message: Message; onAction?: (action: string) => void }) {
  const isUser = message.role === "user";

  return (
    <div className={`dt-message dt-message--${message.role}`}>
      {!isUser && (
        <div className="dt-message-avatar">
          <span className="dt-message-avatar-icon">🤖</span>
        </div>
      )}
      <div className="dt-message-content">
        <div className="dt-message-bubble">
          {message.status === "thinking" ? (
            <TypingIndicator />
          ) : message.status === "executing" ? (
            <div className="dt-message-executing">
              <div className="dt-loader" style={{ width: 20, height: 20 }} />
              <span>Executing workflow...</span>
            </div>
          ) : (
            <p className="dt-message-text">{message.content}</p>
          )}
        </div>
        {message.actions && message.actions.length > 0 && (
          <div className="dt-message-actions">
            {message.actions.map((action, i) => (
              <button
                key={i}
                className="dt-btn dt-btn-neon"
                onClick={() => onAction?.(action.action)}
              >
                {action.label}
              </button>
            ))}
          </div>
        )}
        <span className="dt-message-time">
          {message.timestamp.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })}
        </span>
      </div>
      {isUser && (
        <div className="dt-message-avatar dt-message-avatar--user">
          <span className="dt-message-avatar-icon">👤</span>
        </div>
      )}
    </div>
  );
}

export function AICopilot({ messages, onSend, onAction, isThinking }: AICopilotProps) {
  const [input, setInput] = useState("");
  const [isExpanded, setIsExpanded] = useState(true);
  const messagesEndRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (input.trim() && !isThinking) {
      onSend(input.trim());
      setInput("");
    }
  };

  const handleSuggestion = (suggestion: string) => {
    setInput(suggestion);
  };

  if (!isExpanded) {
    return (
      <button className="dt-copilot-fab" onClick={() => setIsExpanded(true)}>
        <span className="dt-copilot-fab-icon">🤖</span>
        <span className="dt-copilot-fab-pulse" />
      </button>
    );
  }

  return (
    <div className="dt-copilot">
      <div className="dt-copilot-header">
        <div className="dt-copilot-header-info">
          <span className="dt-copilot-avatar">🤖</span>
          <div>
            <h3 className="dt-copilot-title">AI Copilot</h3>
            <span className="dt-copilot-status">
              <span className="dt-copilot-status-dot" />
              Online
            </span>
          </div>
        </div>
        <button className="dt-btn dt-btn-ghost dt-btn-icon" onClick={() => setIsExpanded(false)}>
          <svg width="16" height="16" viewBox="0 0 16 16" fill="none">
            <path d="M4 6L8 10L12 6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </button>
      </div>

      <div className="dt-copilot-messages">
        {messages.length === 0 ? (
          <div className="dt-copilot-welcome">
            <div className="dt-copilot-welcome-icon">✨</div>
            <h4>How can I help you today?</h4>
            <p>I can help you move data, create mappings, and automate workflows.</p>
            <div className="dt-copilot-suggestions">
              {SUGGESTIONS.map((suggestion, i) => (
                <button
                  key={i}
                  className="dt-copilot-suggestion"
                  onClick={() => handleSuggestion(suggestion)}
                >
                  {suggestion}
                </button>
              ))}
            </div>
          </div>
        ) : (
          <>
            {messages.map((msg) => (
              <MessageBubble key={msg.id} message={msg} onAction={onAction} />
            ))}
            {isThinking && (
              <MessageBubble
                message={{
                  id: "thinking",
                  role: "assistant",
                  content: "",
                  timestamp: new Date(),
                  status: "thinking",
                }}
              />
            )}
            <div ref={messagesEndRef} />
          </>
        )}
      </div>

      <form className="dt-copilot-input" onSubmit={handleSubmit}>
        <input
          type="text"
          className="dt-input"
          placeholder="Type a command or ask a question..."
          value={input}
          onChange={(e) => setInput(e.target.value)}
          disabled={isThinking}
        />
        <button
          type="submit"
          className="dt-btn dt-btn-primary dt-btn-icon"
          disabled={!input.trim() || isThinking}
        >
          <svg width="18" height="18" viewBox="0 0 18 18" fill="none">
            <path d="M2 9L16 2L9 16L8 10L2 9Z" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round"/>
          </svg>
        </button>
      </form>
    </div>
  );
}

export function AICopilotStyles() {
  return (
    <style>{`
      .dt-copilot {
        position: fixed;
        bottom: var(--dt-space-6);
        right: var(--dt-space-6);
        width: 420px;
        max-height: 600px;
        display: flex;
        flex-direction: column;
        background: var(--dt-surface);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-2xl);
        box-shadow: var(--dt-shadow-lg), 0 0 60px rgba(0, 212, 255, 0.1);
        overflow: hidden;
        z-index: 1000;
        animation: dt-slide-up 0.3s var(--dt-ease);
      }

      @keyframes dt-slide-up {
        from { opacity: 0; transform: translateY(20px); }
        to { opacity: 1; transform: translateY(0); }
      }

      .dt-copilot-fab {
        position: fixed;
        bottom: var(--dt-space-6);
        right: var(--dt-space-6);
        width: 60px;
        height: 60px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: var(--dt-gradient-neon);
        border: none;
        border-radius: 50%;
        cursor: pointer;
        box-shadow: var(--dt-shadow-neon);
        z-index: 1000;
        transition: transform 0.2s var(--dt-ease);
      }

      .dt-copilot-fab:hover {
        transform: scale(1.1);
      }

      .dt-copilot-fab-icon {
        font-size: 24px;
        position: relative;
        z-index: 1;
      }

      .dt-copilot-fab-pulse {
        position: absolute;
        inset: 0;
        border-radius: 50%;
        background: var(--dt-gradient-neon);
        animation: dt-fab-pulse 2s ease-in-out infinite;
      }

      @keyframes dt-fab-pulse {
        0%, 100% { opacity: 0.4; transform: scale(1); }
        50% { opacity: 0; transform: scale(1.5); }
      }

      .dt-copilot-header {
        display: flex;
        align-items: center;
        justify-content: space-between;
        padding: var(--dt-space-4) var(--dt-space-5);
        background: linear-gradient(135deg, rgba(0, 212, 255, 0.1), rgba(123, 97, 255, 0.1));
        border-bottom: 1px solid var(--dt-border);
      }

      .dt-copilot-header-info {
        display: flex;
        align-items: center;
        gap: var(--dt-space-3);
      }

      .dt-copilot-avatar {
        width: 36px;
        height: 36px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: var(--dt-gradient-neon);
        border-radius: var(--dt-radius-md);
        font-size: 18px;
      }

      .dt-copilot-title {
        font-size: var(--dt-text-md);
        font-weight: 600;
        color: var(--dt-text);
        margin: 0;
      }

      .dt-copilot-status {
        display: flex;
        align-items: center;
        gap: var(--dt-space-1);
        font-size: var(--dt-text-xs);
        color: var(--dt-emerald);
      }

      .dt-copilot-status-dot {
        width: 6px;
        height: 6px;
        background: var(--dt-emerald);
        border-radius: 50%;
        animation: dt-pulse 2s ease-in-out infinite;
      }

      .dt-copilot-messages {
        flex: 1;
        overflow-y: auto;
        padding: var(--dt-space-5);
        min-height: 300px;
        max-height: 400px;
      }

      .dt-copilot-welcome {
        display: flex;
        flex-direction: column;
        align-items: center;
        text-align: center;
        padding: var(--dt-space-6);
      }

      .dt-copilot-welcome-icon {
        font-size: 40px;
        margin-bottom: var(--dt-space-4);
      }

      .dt-copilot-welcome h4 {
        font-size: var(--dt-text-lg);
        font-weight: 600;
        color: var(--dt-text);
        margin: 0 0 var(--dt-space-2);
      }

      .dt-copilot-welcome p {
        font-size: var(--dt-text-sm);
        color: var(--dt-text-secondary);
        margin: 0 0 var(--dt-space-5);
      }

      .dt-copilot-suggestions {
        display: flex;
        flex-direction: column;
        gap: var(--dt-space-2);
        width: 100%;
      }

      .dt-copilot-suggestion {
        padding: var(--dt-space-3) var(--dt-space-4);
        font-family: inherit;
        font-size: var(--dt-text-sm);
        color: var(--dt-text-secondary);
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-md);
        cursor: pointer;
        text-align: left;
        transition: all var(--dt-duration-fast) var(--dt-ease);
      }

      .dt-copilot-suggestion:hover {
        background: var(--dt-electric-dim);
        border-color: var(--dt-electric);
        color: var(--dt-electric);
      }

      .dt-message {
        display: flex;
        gap: var(--dt-space-3);
        margin-bottom: var(--dt-space-4);
      }

      .dt-message--user {
        flex-direction: row-reverse;
      }

      .dt-message-avatar {
        width: 32px;
        height: 32px;
        display: flex;
        align-items: center;
        justify-content: center;
        background: var(--dt-purple-dim);
        border-radius: var(--dt-radius-md);
        flex-shrink: 0;
      }

      .dt-message-avatar--user {
        background: var(--dt-electric-dim);
      }

      .dt-message-avatar-icon {
        font-size: 16px;
      }

      .dt-message-content {
        max-width: 80%;
      }

      .dt-message--user .dt-message-content {
        align-items: flex-end;
      }

      .dt-message-bubble {
        padding: var(--dt-space-3) var(--dt-space-4);
        background: rgba(255, 255, 255, 0.05);
        border: 1px solid var(--dt-border);
        border-radius: var(--dt-radius-lg);
      }

      .dt-message--user .dt-message-bubble {
        background: var(--dt-electric-dim);
        border-color: rgba(0, 212, 255, 0.3);
      }

      .dt-message-text {
        font-size: var(--dt-text-sm);
        color: var(--dt-text);
        line-height: 1.5;
        margin: 0;
        white-space: pre-wrap;
      }

      .dt-message-executing {
        display: flex;
        align-items: center;
        gap: var(--dt-space-3);
        font-size: var(--dt-text-sm);
        color: var(--dt-purple);
      }

      .dt-message-actions {
        display: flex;
        flex-wrap: wrap;
        gap: var(--dt-space-2);
        margin-top: var(--dt-space-2);
      }

      .dt-message-time {
        font-size: 10px;
        color: var(--dt-text-muted);
        margin-top: var(--dt-space-1);
      }

      .dt-typing-indicator {
        display: flex;
        align-items: center;
        gap: 4px;
        padding: var(--dt-space-1) 0;
      }

      .dt-typing-indicator span {
        width: 6px;
        height: 6px;
        background: var(--dt-text-muted);
        border-radius: 50%;
        animation: dt-typing 1.4s ease-in-out infinite;
      }

      .dt-typing-indicator span:nth-child(2) {
        animation-delay: 0.2s;
      }

      .dt-typing-indicator span:nth-child(3) {
        animation-delay: 0.4s;
      }

      @keyframes dt-typing {
        0%, 60%, 100% { transform: translateY(0); opacity: 0.4; }
        30% { transform: translateY(-4px); opacity: 1; }
      }

      .dt-copilot-input {
        display: flex;
        gap: var(--dt-space-3);
        padding: var(--dt-space-4);
        border-top: 1px solid var(--dt-border);
        background: rgba(0, 0, 0, 0.2);
      }

      .dt-copilot-input .dt-input {
        flex: 1;
      }
    `}</style>
  );
}
