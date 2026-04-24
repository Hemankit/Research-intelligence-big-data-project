import { useState } from 'react'
import { useStore } from './hooks/useStore'

import Topbar           from './components/Topbar'
import QueryBar         from './components/QueryBar'
import Sidebar          from './components/Sidebar'
import SnapshotCards    from './components/SnapshotCards'
import TrendExplorer    from './components/TrendExplorer'
import LandscapeMap     from './components/LandscapeMap'
import InfluentialPapers from './components/InfluentialPapers'
import RightPanel       from './components/RightPanel'

const TABS = [
  { id: 'trends',    label: 'Trend Explorer'     },
  { id: 'landscape', label: 'Landscape Map'      },
  { id: 'papers',    label: 'Influential Papers'  },
]

export default function App() {
  const { activeTab, setActiveTab } = useStore()
  const [queryResult, setQueryResult] = useState(null)

  return (
    <div style={{ display: 'flex', flexDirection: 'column', height: '100vh', overflow: 'hidden' }}>
      <Topbar />
      <QueryBar onResult={setQueryResult} />

      {/* Query result banner */}
      {queryResult && (
        <div style={{
          background: 'rgba(59,130,246,0.08)',
          borderBottom: '1px solid rgba(59,130,246,0.2)',
          padding: '10px 24px',
          display: 'flex',
          alignItems: 'flex-start',
          gap: 10,
          fontSize: 11,
          color: 'var(--ink2)',
          animation: 'fadeIn 0.3s ease',
          flexShrink: 0,
        }}>
          <span style={{ fontSize: 9, color: 'var(--blue)', letterSpacing: '1px', textTransform: 'uppercase', paddingTop: 1, whiteSpace: 'nowrap' }}>
            RESULT
          </span>
          <span>{queryResult.summary}</span>
          <button
            onClick={() => setQueryResult(null)}
            style={{ marginLeft: 'auto', fontSize: 11, color: 'var(--ink3)', background: 'none', border: 'none', cursor: 'pointer', flexShrink: 0 }}
          >×</button>
        </div>
      )}

      {/* Main layout */}
      <div style={{ display: 'flex', flex: 1, overflow: 'hidden' }}>
        <Sidebar />

        {/* Center column */}
        <div style={{ flex: 1, display: 'flex', flexDirection: 'column', overflow: 'hidden' }}>
          <SnapshotCards />

          {/* Tab bar */}
          <div style={{
            display: 'flex',
            padding: '0 20px',
            borderBottom: '1px solid var(--border)',
            flexShrink: 0,
            background: 'var(--bg2)',
          }}>
            {TABS.map(tab => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id)}
                style={{
                  padding: '10px 16px',
                  fontSize: 11,
                  letterSpacing: '0.3px',
                  color: activeTab === tab.id ? 'var(--blue)' : 'var(--ink3)',
                  background: 'none',
                  border: 'none',
                  borderBottom: `2px solid ${activeTab === tab.id ? 'var(--blue)' : 'transparent'}`,
                  marginBottom: -1,
                  cursor: 'pointer',
                  transition: 'all var(--transition)',
                }}
              >{tab.label}</button>
            ))}
          </div>

          {/* Tab content */}
          <div style={{ flex: 1, overflowY: 'auto', padding: '18px 20px' }}>
            {activeTab === 'trends'    && <TrendExplorer />}
            {activeTab === 'landscape' && <LandscapeMap />}
            {activeTab === 'papers'    && <InfluentialPapers />}
          </div>
        </div>

        <RightPanel />
      </div>
    </div>
  )
}
