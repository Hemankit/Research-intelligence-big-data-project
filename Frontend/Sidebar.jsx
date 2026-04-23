import { useStore } from '../hooks/useStore'
import { useStats } from '../hooks/useData'

const DOMAINS = ['cs.LG','cs.CL','cs.CV','cs.AI','stat.ML','cs.IR','cs.NE']
const PAPER_TYPES = ['Preprint','Conference','Journal','Workshop']

export default function Sidebar() {
  const { filters, setFilter } = useStore()
  const { data: stats } = useStats()

  const toggleDomain = (d) => {
    const cur = filters.domains
    setFilter('domains', cur.includes(d) ? cur.filter(x => x !== d) : [...cur, d])
  }
  const toggleType = (t) => {
    const cur = filters.paperTypes
    setFilter('paperTypes', cur.includes(t) ? cur.filter(x => x !== t) : [...cur, t])
  }

  return (
    <aside style={{
      width: 220,
      background: 'var(--bg2)',
      borderRight: '1px solid var(--border)',
      padding: '16px 14px',
      display: 'flex',
      flexDirection: 'column',
      gap: 20,
      overflowY: 'auto',
      flexShrink: 0,
    }}>
      <Section title="Domain">
        <ChipGroup>
          {DOMAINS.map(d => (
            <Chip
              key={d}
              active={filters.domains.includes(d)}
              onClick={() => toggleDomain(d)}
            >{d}</Chip>
          ))}
        </ChipGroup>
      </Section>

      <Section title="Paper Type">
        <ChipGroup>
          {PAPER_TYPES.map(t => (
            <Chip
              key={t}
              active={filters.paperTypes.includes(t)}
              onClick={() => toggleType(t)}
            >{t}</Chip>
          ))}
        </ChipGroup>
      </Section>

      <Section title="Time Window">
        <SliderRow
          label="From"
          min={2018} max={2024}
          value={filters.fromYear}
          onChange={v => setFilter('fromYear', v)}
          format={v => v}
        />
        <SliderRow
          label="To"
          min={2019} max={2025}
          value={filters.toYear}
          onChange={v => setFilter('toYear', v)}
          format={v => v}
        />
      </Section>

      <Section title="Influence">
        <SliderRow
          label="Min PageRank"
          min={0} max={100}
          value={filters.minPagerank}
          onChange={v => setFilter('minPagerank', v)}
          format={v => `${v}%`}
        />
      </Section>

      {/* Corpus stats */}
      <Section title="Corpus">
        <div style={{ display: 'flex', flexDirection: 'column', gap: 5 }}>
          {[
            ['Papers',       stats ? (stats.total_papers / 1e6).toFixed(2) + 'M' : '—'],
            ['Full-text',    stats ? (stats.full_text_papers / 1e6).toFixed(2) + 'M' : '—'],
            ['Cite edges',   stats ? (stats.citation_edges / 1e6).toFixed(1) + 'M' : '—'],
            ['Clusters',     stats?.topic_clusters ?? '—'],
            ['Last ingest',  stats ? '2h ago' : '—'],
          ].map(([k, v]) => (
            <div key={k} style={{ display: 'flex', justifyContent: 'space-between', fontSize: 10 }}>
              <span style={{ color: 'var(--ink3)' }}>{k}</span>
              <span style={{ color: 'var(--ink2)' }}>{v}</span>
            </div>
          ))}
        </div>
      </Section>
    </aside>
  )
}

function Section({ title, children }) {
  return (
    <div>
      <div style={{
        fontSize: 9, letterSpacing: '1.5px', textTransform: 'uppercase',
        color: 'var(--ink3)', marginBottom: 8,
        paddingBottom: 6, borderBottom: '1px solid var(--border)',
      }}>{title}</div>
      {children}
    </div>
  )
}

function ChipGroup({ children }) {
  return <div style={{ display: 'flex', flexWrap: 'wrap', gap: 5 }}>{children}</div>
}

function Chip({ active, onClick, children }) {
  return (
    <button
      onClick={onClick}
      style={{
        padding: '4px 8px',
        borderRadius: 'var(--radius-sm)',
        fontSize: 10,
        cursor: 'pointer',
        border: `1px solid ${active ? 'var(--blue)' : 'var(--border)'}`,
        background: active ? 'var(--blue-dim)' : 'transparent',
        color: active ? 'var(--blue)' : 'var(--ink3)',
        transition: 'all var(--transition)',
      }}
    >{children}</button>
  )
}

function SliderRow({ label, min, max, value, onChange, format }) {
  return (
    <div style={{ marginBottom: 10 }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
        <span style={{ fontSize: 10, color: 'var(--ink3)' }}>{label}</span>
        <span style={{ fontSize: 10, color: 'var(--ink2)' }}>{format(value)}</span>
      </div>
      <input
        type="range" min={min} max={max} value={value} step={1}
        onChange={e => onChange(Number(e.target.value))}
        style={{ width: '100%', accentColor: 'var(--blue)' }}
      />
    </div>
  )
}
