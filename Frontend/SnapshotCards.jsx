import { useStats, useTrendingTopics } from '../hooks/useData'

export default function SnapshotCards() {
  const { data: stats } = useStats()
  const { data: trending } = useTrendingTopics()

  const fastest = trending?.[0]

  const cards = [
    {
      label:  'Trending Topics',
      value:  stats?.topic_clusters ?? '—',
      sub:    '+12 this week',
      delta:  'up',
      accent: 'var(--blue)',
    },
    {
      label:  'Fastest Growing',
      value:  fastest?.name?.split(' ')[0] ?? 'LoRA',
      sub:    fastest?.delta ?? '+340% 6mo',
      delta:  'up',
      accent: 'var(--green)',
    },
    {
      label:  'Top Dataset',
      value:  'ImageNet',
      sub:    '14.2K citing papers',
      delta:  null,
      accent: 'var(--amber)',
    },
    {
      label:  'New This Week',
      value:  stats ? (stats.papers_today * 7).toLocaleString() : '5,921',
      sub:    '+8.3% vs last week',
      delta:  'up',
      accent: 'var(--teal)',
    },
  ]

  return (
    <div style={{
      display: 'grid',
      gridTemplateColumns: 'repeat(4, 1fr)',
      gap: 12,
      padding: '14px 20px',
      borderBottom: '1px solid var(--border)',
      flexShrink: 0,
    }}>
      {cards.map((c, i) => (
        <div
          key={c.label}
          style={{
            background: 'var(--bg2)',
            border: '1px solid var(--border)',
            borderRadius: 'var(--radius-md)',
            padding: '12px 14px',
            position: 'relative',
            overflow: 'hidden',
            animation: `fadeUp 0.4s ease ${i * 0.06}s both`,
          }}
        >
          {/* top accent bar */}
          <div style={{
            position: 'absolute', top: 0, left: 0, right: 0, height: 2,
            background: c.accent,
          }} />
          <div style={{ fontSize: 9, letterSpacing: '1px', textTransform: 'uppercase', color: 'var(--ink3)', marginBottom: 6 }}>
            {c.label}
          </div>
          <div style={{
            fontFamily: 'var(--font-display)',
            fontSize: 26,
            fontWeight: 400,
            color: 'var(--ink)',
            lineHeight: 1,
            marginBottom: 4,
          }}>{c.value}</div>
          <div style={{ fontSize: 10, color: 'var(--ink3)', display: 'flex', alignItems: 'center', gap: 5 }}>
            {c.delta && (
              <span style={{
                fontSize: 10,
                padding: '1px 5px',
                borderRadius: 3,
                background: c.delta === 'up' ? 'var(--green-dim)' : 'rgba(248,113,113,0.12)',
                color: c.delta === 'up' ? 'var(--green)' : 'var(--coral)',
              }}>↑</span>
            )}
            {c.sub}
          </div>
        </div>
      ))}
    </div>
  )
}
