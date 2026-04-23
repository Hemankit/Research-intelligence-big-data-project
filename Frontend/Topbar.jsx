import { useStats } from '../hooks/useData'

export default function Topbar() {
  const { data: stats } = useStats()

  return (
    <header style={{
      background: 'var(--bg)',
      borderBottom: '1px solid var(--border)',
      padding: '0 24px',
      height: 52,
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'space-between',
      position: 'sticky',
      top: 0,
      zIndex: 200,
      flexShrink: 0,
    }}>
      {/* Logo */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        <div style={{
          width: 30, height: 30,
          background: 'var(--blue)',
          borderRadius: 'var(--radius-sm)',
          display: 'flex', alignItems: 'center', justifyContent: 'center',
          fontSize: 14, fontWeight: 500, color: '#fff',
          letterSpacing: '-0.5px',
          fontFamily: 'var(--font-display)',
          fontStyle: 'italic',
        }}>R</div>
        <div>
          <div style={{
            fontFamily: 'var(--font-display)',
            fontSize: 16,
            fontWeight: 400,
            color: 'var(--ink)',
            letterSpacing: '-0.3px',
          }}>Research Intelligence</div>
          <div style={{ fontSize: 9, color: 'var(--ink3)', letterSpacing: '1.5px', textTransform: 'uppercase' }}>
            CS586 / DS504
          </div>
        </div>
      </div>

      {/* Right */}
      <div style={{ display: 'flex', alignItems: 'center', gap: 12 }}>
        {stats && (
          <span style={{ fontSize: 11, color: 'var(--ink3)' }}>
            {(stats.total_papers / 1e6).toFixed(1)}M papers · {(stats.citation_edges / 1e6).toFixed(1)}M edges
          </span>
        )}
        <LiveBadge count={stats?.papers_today} />
        <span style={{
          padding: '4px 10px',
          border: '1px solid var(--border2)',
          borderRadius: 'var(--radius-sm)',
          fontSize: 10,
          color: 'var(--ink3)',
          letterSpacing: '0.5px',
        }}>HDFS · Spark · ES</span>
      </div>
    </header>
  )
}

function LiveBadge({ count }) {
  return (
    <div style={{
      display: 'flex', alignItems: 'center', gap: 6,
      padding: '4px 10px',
      background: 'rgba(16,185,129,0.1)',
      border: '1px solid rgba(16,185,129,0.2)',
      borderRadius: 'var(--radius-sm)',
      fontSize: 10,
      color: 'var(--green)',
      letterSpacing: '0.5px',
    }}>
      <span style={{
        width: 5, height: 5, borderRadius: '50%',
        background: 'var(--green)',
        animation: 'pulse-dot 2s infinite',
        display: 'inline-block',
      }} />
      LIVE{count ? ` · ${count.toLocaleString()} today` : ''}
    </div>
  )
}
