import { useState } from 'react'
import { useStore } from '../hooks/useStore'
import { useNLQuery } from '../hooks/useData'
import { Search, Loader } from 'lucide-react'

const PRESETS = [
  'Emerging trends in GNNs over 6 months',
  'Datasets used in diffusion research',
  'LLM research clusters 2024',
  'Transformer efficiency methods',
]

export default function QueryBar({ onResult }) {
  const { query, setQuery } = useStore()
  const { filters } = useStore()
  const { submit, loading } = useNLQuery()
  const [focused, setFocused] = useState(false)

  const run = async () => {
    if (!query.trim()) return
    const result = await submit(query, filters)
    if (onResult) onResult(result)
  }

  return (
    <div style={{
      background: 'var(--bg2)',
      borderBottom: '1px solid var(--border)',
      padding: '10px 24px',
      display: 'flex',
      alignItems: 'center',
      gap: 12,
      flexShrink: 0,
    }}>
      <span style={{ fontSize: 9, color: 'var(--ink3)', letterSpacing: '1.5px', textTransform: 'uppercase', whiteSpace: 'nowrap' }}>
        QUERY
      </span>

      <div style={{
        flex: 1,
        display: 'flex',
        alignItems: 'center',
        background: 'var(--bg3)',
        border: `1px solid ${focused ? 'rgba(59,130,246,0.4)' : 'var(--border)'}`,
        borderRadius: 'var(--radius-md)',
        padding: '0 12px',
        gap: 8,
        transition: 'border-color var(--transition)',
      }}>
        {loading
          ? <Loader size={13} color="var(--ink3)" style={{ animation: 'spin 1s linear infinite', flexShrink: 0 }} />
          : <Search size={13} color="var(--ink3)" style={{ flexShrink: 0 }} />
        }
        <input
          value={query}
          onChange={e => setQuery(e.target.value)}
          onFocus={() => setFocused(true)}
          onBlur={() => setFocused(false)}
          onKeyDown={e => e.key === 'Enter' && run()}
          placeholder='e.g. "What are the emerging trends in Graph Neural Networks over the past 6 months?"'
          style={{
            flex: 1,
            background: 'none',
            border: 'none',
            outline: 'none',
            fontSize: 12,
            color: 'var(--ink)',
            padding: '8px 0',
          }}
        />
      </div>

      <button
        onClick={run}
        disabled={loading}
        style={{
          padding: '8px 16px',
          background: loading ? 'var(--bg4)' : 'var(--blue)',
          color: '#fff',
          border: 'none',
          borderRadius: 'var(--radius-md)',
          fontSize: 11,
          cursor: loading ? 'not-allowed' : 'pointer',
          letterSpacing: '0.3px',
          whiteSpace: 'nowrap',
          transition: 'background var(--transition)',
        }}
      >
        {loading ? 'Analyzing...' : 'Explore ↗'}
      </button>

      <div style={{ display: 'flex', gap: 6 }}>
        {PRESETS.map(p => (
          <button
            key={p}
            onClick={() => setQuery(p)}
            style={{
              padding: '5px 10px',
              background: 'var(--bg4)',
              border: '1px solid var(--border)',
              borderRadius: 100,
              fontSize: 10,
              color: 'var(--ink3)',
              cursor: 'pointer',
              whiteSpace: 'nowrap',
              transition: 'all var(--transition)',
            }}
            onMouseEnter={e => { e.target.style.color = 'var(--ink)'; e.target.style.borderColor = 'var(--border2)' }}
            onMouseLeave={e => { e.target.style.color = 'var(--ink3)'; e.target.style.borderColor = 'var(--border)' }}
          >{p}</button>
        ))}
      </div>

      <style>{`@keyframes spin { to { transform: rotate(360deg); } }`}</style>
    </div>
  )
}
