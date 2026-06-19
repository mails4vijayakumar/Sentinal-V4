import { NavLink, Outlet, Route, Routes, Navigate } from 'react-router-dom'
import { Activity, BarChart2, MessageSquare, Shield } from 'lucide-react'
import { LiveView }    from './routes/LiveView'
import { ReportsView } from './routes/ReportsView'
import { ChatPage }    from './routes/ChatPage'

const NAV = [
  { to: '/live',    icon: Activity,      label: 'Live' },
  { to: '/reports', icon: BarChart2,     label: 'Reports' },
  { to: '/chat',    icon: MessageSquare, label: 'Chat' },
]

export default function App() {
  return (
    <div className="app-shell">
      {/* Header */}
      <header className="app-header" style={{
        display: 'flex', alignItems: 'center',
        padding: '0 20px', borderBottom: '1px solid var(--border)',
        background: 'var(--bg-surface)', gap: 12,
        position: 'relative', zIndex: 10,
      }}>
        <Shield size={18} color="var(--amber)" style={{ filter: 'drop-shadow(0 0 6px rgba(245,166,35,.5))' }} />
        <span style={{ fontFamily: 'var(--font-mono)', fontSize: 13, fontWeight: 600,
          color: 'var(--amber)', letterSpacing: '0.06em', textShadow: 'var(--amber-text-glow)' }}>
          SENTINEL
        </span>
        <span style={{ fontFamily: 'var(--font-mono)', fontSize: 11, color: 'var(--text-muted)',
          letterSpacing: '0.1em', textTransform: 'uppercase' }}>
          Incident Orchestrator
        </span>
        {/* Decorative scanline */}
        <div style={{ position: 'absolute', right: 20, top: '50%', transform: 'translateY(-50%)',
          display: 'flex', alignItems: 'center', gap: 6 }}>
          <div style={{ width: 6, height: 6, borderRadius: '50%', background: 'var(--green)',
            boxShadow: 'var(--green-glow)', animation: 'pulse-glow 2s infinite' }} />
          <span style={{ fontFamily: 'var(--font-mono)', fontSize: 10, color: 'var(--green)' }}>ONLINE</span>
        </div>
      </header>

      {/* Sidebar */}
      <nav className="app-sidebar" style={{
        padding: '20px 0', display: 'flex', flexDirection: 'column', gap: 2,
        borderRight: '1px solid var(--border)', background: 'var(--bg-surface)',
      }}>
        <div style={{ padding: '0 12px 16px', borderBottom: '1px solid var(--border)', marginBottom: 8 }}>
          <span style={{ fontFamily: 'var(--font-mono)', fontSize: 10, color: 'var(--text-muted)',
            textTransform: 'uppercase', letterSpacing: '0.12em' }}>Navigation</span>
        </div>
        {NAV.map(({ to, icon: Icon, label }) => (
          <NavLink
            key={to}
            to={to}
            style={({ isActive }) => ({
              display: 'flex', alignItems: 'center', gap: 10,
              padding: '9px 16px', margin: '0 8px', borderRadius: 6,
              textDecoration: 'none', fontSize: 13, fontWeight: 500,
              transition: 'all 120ms ease',
              color: isActive ? 'var(--amber)' : 'var(--text-secondary)',
              background: isActive ? 'var(--amber-dim)' : 'transparent',
              borderLeft: isActive ? '2px solid var(--amber)' : '2px solid transparent',
            })}
          >
            <Icon size={15} />
            {label}
          </NavLink>
        ))}
        {/* Agent status strip */}
        <div style={{ marginTop: 'auto', padding: '16px 16px 0', borderTop: '1px solid var(--border)' }}>
          <div style={{ fontFamily: 'var(--font-mono)', fontSize: 10, color: 'var(--text-muted)',
            textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 10 }}>Agents</div>
          <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
            {[1,2,3,4,5,6,7].map(n => (
              <div key={n} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between' }}>
                <span style={{ fontFamily: 'var(--font-mono)', fontSize: 10, color: 'var(--text-muted)' }}>
                  A{n}
                </span>
                <div style={{ width: 5, height: 5, borderRadius: '50%', background: 'var(--green)' }} />
              </div>
            ))}
          </div>
        </div>
      </nav>

      {/* Main content */}
      <main className="app-main">
        <Routes>
          <Route path="/"        element={<Navigate to="/live" replace />} />
          <Route path="/live"    element={<LiveView />} />
          <Route path="/reports" element={<ReportsView />} />
          <Route path="/chat"    element={<ChatPage />} />
        </Routes>
      </main>
    </div>
  )
}
