import { useState, useEffect, useRef, useCallback } from 'react'
import './App.css'

interface ChatMessage {
  id: number
  text: string
  image?: string
  isOwn: boolean
  timestamp: Date
}

type ConnectionStatus = 'connecting' | 'connected' | 'disconnected'

function App() {
  const [messages, setMessages] = useState<ChatMessage[]>([])
  const [inputValue, setInputValue] = useState('')
  const [status, setStatus] = useState<ConnectionStatus>('disconnected')
  const [username, setUsername] = useState('')
  const [dmTarget, setDmTarget] = useState<string | null>(null)
  const [activeUsers, setActiveUsers] = useState<string[]>([])
  const [availableRooms, setAvailableRooms] = useState<string[]>([])
  const [currentRoom, setCurrentRoom] = useState<string>('lobby')
  const [newRoomName, setNewRoomName] = useState<string>('')
  const [highlightedMessageId, setHighlightedMessageId] = useState<number | null>(null)
  const [showSentNotice, setShowSentNotice] = useState(false)
  const [usersPanelExpanded, setUsersPanelExpanded] = useState(false)
  const [roomsPanelExpanded, setRoomsPanelExpanded] = useState(true)
  const wsRef = useRef<WebSocket | null>(null)
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const messageIdRef = useRef(0)
  const sentNoticeTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null)

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' })
  }

  useEffect(() => {
    scrollToBottom()
  }, [messages])

  // Set default panel states based on screen size
  useEffect(() => {
    const checkScreenSize = () => {
      const isMobile = window.innerWidth < 768
      setUsersPanelExpanded(!isMobile)
      setRoomsPanelExpanded(!isMobile)
    }
    
    checkScreenSize()
    window.addEventListener('resize', checkScreenSize)
    return () => window.removeEventListener('resize', checkScreenSize)
  }, [])

  function handleFileChange(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0];
    if (!file || !wsRef.current) return;

    const reader = new FileReader();
    reader.onload = () => {
      const imgData = reader.result as string;
      wsRef.current?.send(
        JSON.stringify({ type: "image", data: imgData })
      );
      // Add the image as own message
      const newMessage: ChatMessage = {
        id: messageIdRef.current++,
        text: '',
        image: imgData,
        isOwn: true,
        timestamp: new Date()
      };
      setMessages(prev => [...prev, newMessage]);
      setHighlightedMessageId(newMessage.id)
      triggerSentNotice()
    };
    reader.readAsDataURL(file);
  }

  const connect = useCallback(() => {
    if (wsRef.current?.readyState === WebSocket.OPEN) return

    setStatus('connecting')
    const ws = new WebSocket('ws://127.0.0.1:8080')

    ws.onopen = () => {
      setStatus('connected')
      if (username) {
        ws.send(`/name ${username}`)
      }
      ws.send('/who')
      ws.send('/get_rooms')
      setCurrentRoom('lobby')
    }

    ws.onmessage = (event) => {
      if (typeof event.data === 'string') {
        const trimmed = event.data.trim()
        const lowered = trimmed.toLowerCase()
        if (lowered.startsWith('online:')) {
          const list = trimmed
            .slice('online:'.length)
            .split(',')
            .map(name => name.trim())
            .filter(Boolean)
          setActiveUsers(list)
          return
        } else if (lowered === 'no one') {
          setActiveUsers([])
          return
        } else if (trimmed.startsWith('Active rooms: ')) {
          // Parse: "Active rooms: [\"lobby\", \"room1\", \"room2\"]"
          try {
            const roomsStr = trimmed.slice('Active rooms: '.length)
            const rooms: string[] = JSON.parse(roomsStr)
            // Deduplicate and ensure lobby appears only once
            const uniqueRooms = Array.from(new Set(rooms))
            // Always include lobby if not present, and put it first
            if (!uniqueRooms.includes('lobby')) {
              uniqueRooms.unshift('lobby')
            } else {
              // Move lobby to front if it exists
              const lobbyIndex = uniqueRooms.indexOf('lobby')
              uniqueRooms.splice(lobbyIndex, 1)
              uniqueRooms.unshift('lobby')
            }
            setAvailableRooms(uniqueRooms)
          } catch (e) {
            console.error('Failed to parse rooms:', e)
            setAvailableRooms(['lobby'])
          }
          return
        } else if (trimmed.startsWith('Joined room: ')) {
          const roomName = trimmed.slice('Joined room: '.length)
          setCurrentRoom(roomName)
          setMessages([]) // Clear messages when joining a new room
          return
        }
      }
      let newMessage: ChatMessage;
      
      // Try to parse as JSON (for image messages)
      try {
        const parsed = JSON.parse(event.data);
        if (parsed.type === 'image' && parsed.data) {
          newMessage = {
            id: messageIdRef.current++,
            text: '',
            image: parsed.data,
            isOwn: false,
            timestamp: new Date()
          };
        } else {
          // Unknown JSON format, treat as text
          newMessage = {
            id: messageIdRef.current++,
            text: event.data,
            isOwn: false,
            timestamp: new Date()
          };
        }
      } catch {
        // Not JSON, treat as regular text message
        newMessage = {
          id: messageIdRef.current++,
          text: event.data,
          isOwn: false,
          timestamp: new Date()
        };
      }
      
      setMessages(prev => [...prev, newMessage]);
    }

    ws.onclose = (event) => {
      console.log('WebSocket closed:', event.code, event.reason)
      setStatus('disconnected')
      wsRef.current = null
    }

    ws.onerror = (error) => {
      console.error('WebSocket error:', error)
      setStatus('disconnected')
    }

    wsRef.current = ws
  }, [username])

  const disconnect = () => {
    wsRef.current?.close()
    wsRef.current = null
    setStatus('disconnected')
    setDmTarget(null)
    setActiveUsers([])
    setAvailableRooms([])
    setCurrentRoom('lobby')
    setMessages([])
  }

  const parseDmCommand = (value: string) => {
    if (!value.startsWith('/dm ')) return null
    const payload = value.slice(4).trim()
    if (!payload) return null
    const spaceIndex = payload.indexOf(' ')
    if (spaceIndex === -1) {
      return { target: payload, body: '' }
    }
    const target = payload.slice(0, spaceIndex).trim()
    const body = payload.slice(spaceIndex + 1).trim()
    if (!target) return null
    return { target, body }
  }

  const clearDmTarget = () => setDmTarget(null)
  const requestActiveUsers = () => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send('/who')
    }
  }
  const requestRooms = () => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send('/get_rooms')
    }
  }
  const joinRoom = (roomName: string) => {
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      wsRef.current.send(`/join ${roomName}`)
      setMessages([]) // Clear messages immediately for better UX
      setDmTarget(null) // Clear DM target when switching rooms
    }
  }
  const leaveRoom = () => {
    joinRoom('lobby')
  }
  const createRoom = (e: React.FormEvent) => {
    e.preventDefault()
    const roomName = newRoomName.trim().toLowerCase()
    if (!roomName || roomName === 'lobby') {
      return // Don't allow creating lobby or empty names
    }
    if (wsRef.current && wsRef.current.readyState === WebSocket.OPEN) {
      joinRoom(roomName)
      setNewRoomName('')
      // Refresh rooms list after a short delay to get the new room
      setTimeout(() => {
        requestRooms()
      }, 500)
    }
  }

  const triggerSentNotice = () => {
    setShowSentNotice(true)
    if (sentNoticeTimeoutRef.current) {
      clearTimeout(sentNoticeTimeoutRef.current)
    }
    sentNoticeTimeoutRef.current = setTimeout(() => {
      setShowSentNotice(false)
    }, 2000)
  }

  useEffect(() => {
    return () => {
      if (sentNoticeTimeoutRef.current) {
        clearTimeout(sentNoticeTimeoutRef.current)
      }
    }
  }, [])

  useEffect(() => {
    if (highlightedMessageId !== null) {
      const timeout = setTimeout(() => setHighlightedMessageId(null), 1500)
      return () => clearTimeout(timeout)
    }
  }, [highlightedMessageId])

  const sendMessage = (e: React.FormEvent) => {
    e.preventDefault()
    if (!inputValue.trim() || !wsRef.current || status !== 'connected') return

    let outgoing = inputValue.trim()
    let shouldAddOwnMessage = false
    let ownMessageText = inputValue.trim()

    const typedDm = parseDmCommand(outgoing)

    if (typedDm) {
      setDmTarget(typedDm.target)
      if (typedDm.body) {
        shouldAddOwnMessage = true
        ownMessageText = `(DM to ${typedDm.target}) ${typedDm.body}`
      } else {
        ownMessageText = ''
      }
    } else if (dmTarget) {
      outgoing = `/dm ${dmTarget} ${outgoing}`
      shouldAddOwnMessage = true
      ownMessageText = `(DM to ${dmTarget}) ${inputValue.trim()}`
    } else if (!outgoing.startsWith('/')) {
      shouldAddOwnMessage = true
    }

    wsRef.current.send(outgoing)

    if (shouldAddOwnMessage && ownMessageText) {
      const newMessage: ChatMessage = {
        id: messageIdRef.current++,
        text: ownMessageText,
        isOwn: true,
        timestamp: new Date()
      }
      setMessages(prev => [...prev, newMessage])
      setHighlightedMessageId(newMessage.id)
      triggerSentNotice()
    }
    
    setInputValue('')
  }

  const formatTime = (date: Date) => {
    return date.toLocaleTimeString('en-US', { 
      hour: '2-digit', 
      minute: '2-digit',
      hour12: false 
    })
  }

  return (
    <div className="chat-container">
      <header className="chat-header">
        <div className="header-left">
          <div className="logo">
            <span className="logo-icon">◈</span>
            <span className="logo-text">RELAY</span>
          </div>
          <div className={`status-badge ${status}`}>
            <span className="status-dot"></span>
            {status}
          </div>
          {status === 'connected' && currentRoom && (
            <div className="room-badge">
              <span className="room-icon">🏠</span>
              <span className="room-name">{currentRoom}</span>
              {currentRoom !== 'lobby' && (
                <button 
                  type="button"
                  className="leave-room-btn"
                  onClick={leaveRoom}
                  title="Leave room"
                >
                  ×
                </button>
              )}
            </div>
          )}
        </div>
        <div className="header-right">
          <input
            type="text"
            className="username-input"
            placeholder="Your name..."
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            onBlur={() => {
              if (wsRef.current && username) {
                wsRef.current.send(`/name ${username}`)
              }
            }}
          />
          {status === 'connected' ? (
            <button className="connect-btn disconnect" onClick={disconnect}>
              Disconnect
            </button>
          ) : (
            <button 
              className="connect-btn" 
              onClick={connect}
              disabled={status === 'connecting'}
            >
              {status === 'connecting' ? 'Connecting...' : 'Connect'}
            </button>
          )}
        </div>
      </header>

      <section className={`active-users-panel ${usersPanelExpanded ? 'expanded' : 'collapsed'}`}>
        <div className="active-users-header" onClick={() => setUsersPanelExpanded(!usersPanelExpanded)}>
          <div className="panel-title-wrapper">
            <button 
              type="button" 
              className="panel-toggle-btn"
              aria-label={usersPanelExpanded ? 'Collapse' : 'Expand'}
            >
              <svg 
                viewBox="0 0 24 24" 
                fill="none" 
                stroke="currentColor" 
                strokeWidth="2"
                className={usersPanelExpanded ? 'expanded' : ''}
              >
                <polyline points="6 9 12 15 18 9"></polyline>
              </svg>
            </button>
            <span className="panel-title">Active users</span>
            {activeUsers.length > 0 && (
              <span className="panel-count">({activeUsers.length})</span>
            )}
          </div>
          {usersPanelExpanded && (
            <button 
              type="button" 
              className="refresh-btn" 
              onClick={(e) => {
                e.stopPropagation()
                requestActiveUsers()
              }}
              disabled={status !== 'connected'}
            >
              Refresh
            </button>
          )}
        </div>
        {usersPanelExpanded && (
          <div className="active-users-list">
            {activeUsers.length === 0 ? (
              <span className="empty-users">
                {status === 'connected' ? 'No users online' : 'Connect to see online users'}
              </span>
            ) : (
              activeUsers.map(user => (
                <button
                  key={user}
                  type="button"
                  className={`user-chip ${dmTarget === user ? 'active' : ''}`}
                  onClick={() => setDmTarget(user)}
                >
                  {user}
                  {dmTarget === user && <span className="chip-subtle">DM</span>}
                </button>
              ))
            )}
          </div>
        )}
      </section>

      <section className={`rooms-panel ${roomsPanelExpanded ? 'expanded' : 'collapsed'}`}>
        <div className="rooms-header" onClick={() => setRoomsPanelExpanded(!roomsPanelExpanded)}>
          <div className="panel-title-wrapper">
            <button 
              type="button" 
              className="panel-toggle-btn"
              aria-label={roomsPanelExpanded ? 'Collapse' : 'Expand'}
            >
              <svg 
                viewBox="0 0 24 24" 
                fill="none" 
                stroke="currentColor" 
                strokeWidth="2"
                className={roomsPanelExpanded ? 'expanded' : ''}
              >
                <polyline points="6 9 12 15 18 9"></polyline>
              </svg>
            </button>
            <span className="panel-title">Rooms</span>
            {availableRooms.length > 0 && (
              <span className="panel-count">({availableRooms.length})</span>
            )}
          </div>
          {roomsPanelExpanded && (
            <button 
              type="button" 
              className="refresh-btn" 
              onClick={(e) => {
                e.stopPropagation()
                requestRooms()
              }}
              disabled={status !== 'connected'}
            >
              Refresh
            </button>
          )}
        </div>
        {roomsPanelExpanded && (
          <>
            <div className="rooms-list">
              {availableRooms.length === 0 ? (
                <span className="empty-users">
                  {status === 'connected' ? 'No rooms available' : 'Connect to see rooms'}
                </span>
              ) : (
                availableRooms.map(room => (
                  <button
                    key={room}
                    type="button"
                    className={`room-chip ${currentRoom === room ? 'active' : ''}`}
                    onClick={() => joinRoom(room)}
                  >
                    {room === 'lobby' ? '🏠' : '🚪'} {room}
                    {currentRoom === room && <span className="chip-subtle">Active</span>}
                  </button>
                ))
              )}
            </div>
            {status === 'connected' && (
              <form onSubmit={createRoom} className="create-room-form">
                <input
                  type="text"
                  className="create-room-input"
                  placeholder="Create new room..."
                  value={newRoomName}
                  onChange={(e) => setNewRoomName(e.target.value)}
                  maxLength={20}
                />
                <button
                  type="submit"
                  className="create-room-btn"
                  disabled={!newRoomName.trim() || newRoomName.trim().toLowerCase() === 'lobby'}
                  title="Create and join room"
                >
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                    <line x1="12" y1="5" x2="12" y2="19"></line>
                    <line x1="5" y1="12" x2="19" y2="12"></line>
                  </svg>
                </button>
              </form>
            )}
          </>
        )}
      </section>

      <main className="chat-messages">
        {messages.length === 0 ? (
          <div className="empty-state">
            <div className="empty-icon">💬</div>
            <p>No messages yet</p>
            <span>
              {status === 'connected' 
                ? `Start chatting in ${currentRoom}` 
                : 'Connect to start chatting'}
            </span>
          </div>
        ) : (
          messages.map((msg) => (
            <div 
              key={msg.id} 
              className={`message ${msg.isOwn ? 'own' : ''} ${highlightedMessageId === msg.id ? 'highlight' : ''}`}
            >
              <div className="message-content">
                {msg.image ? (
                  <img 
                    src={msg.image} 
                    alt="Shared image" 
                    className="message-image"
                  />
                ) : (
                  <p>{msg.text}</p>
                )}
                <span className="message-time">{formatTime(msg.timestamp)}</span>
              </div>
            </div>
          ))
        )}
        <div ref={messagesEndRef} />
      </main>

      <footer className="chat-input-area">
        <form onSubmit={sendMessage} className="input-form">
          <div className="input-wrapper">
            <input
              type="text"
              className="message-input"
              placeholder={status === 'connected' ? "Type a message..." : "Connect to chat..."}
              value={inputValue}
              onChange={(e) => setInputValue(e.target.value)}
              disabled={status !== 'connected'}
            />
            <button 
              type="submit" 
              className="send-btn"
              disabled={status !== 'connected' || !inputValue.trim()}
            >
              <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                <path d="M22 2L11 13M22 2L15 22L11 13M22 2L2 9L11 13" />
              </svg>
            </button>
            <div className="file-input-wrapper">
              <input 
                type="file"
                accept="image/*"
                onChange={handleFileChange}
                className="file-input"
                disabled={status !== 'connected'}
              />
              <div className="file-upload-btn">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2">
                  <rect x="3" y="3" width="18" height="18" rx="2" ry="2" />
                  <circle cx="8.5" cy="8.5" r="1.5" />
                  <polyline points="21 15 16 10 5 21" />
                </svg>
              </div>
            </div>
          </div>
          <div className="input-hints">
            <span>/ping</span>
            <span>/who</span>
            <span>/name [name]</span>
          </div>
        </form>
        {dmTarget && (
          <div className="dm-status">
            <span>DM connected to <strong>{dmTarget}</strong></span>
            <button 
              type="button" 
              className="dm-disconnect-btn"
              onClick={clearDmTarget}
            >
              Disconnect DM
            </button>
          </div>
        )}
        {showSentNotice && (
          <div className="sent-notice">
            <span>Message sent</span>
          </div>
        )}
      </footer>
    </div>
  )
}

export default App

