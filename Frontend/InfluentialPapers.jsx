import { useState } from 'react'
import { useInfluentialPapers } from '../hooks/useData'
import { useStore } from '../hooks/useStore'
import { ExternalLink, ChevronDown, ChevronUp } from 'lucide-react'

const CATEGORY_COLORS = {
  Transformers: '#3b82f6', PEFT: '#10b981', Diffusion: '#a78bfa',
  RLHF: '#2dd4bf', RAG: '#f59e0b', Efficiency: '#f87171',
  Vision: '#10b981', Alignment: '#a78bfa',
}

const SORT_OPTIONS = [
  { value: 'pr',     label: 'PageRank' },
  { value: 'cite',   label: 'Citations' },
  { value: 'recent', label: 'Recency' },
]

export default function InfluentialPapers() {
  const { filters, selectedPaper, setSelectedPaper } = useStore()
  const { data: papers, loading } = useInfluentialPapers(filters)
  const [sort, setSort] = useState('pr')

  const sorted = papers ? [...papers].sort((a, b) => {
    if (sort === 'pr')     return b.pagerank - a.pagerank
    if (sort === 'cite')   return b.citations - a.citations
    if (sort === 'recent') return b.year - a.year
    return 0
  }) : []

  return (
    <div>
      <div style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', marginBottom: 12 }}>
        <div style={{ fontFamily: 'var(--font-display)', fontSize: 15 }}>
          Influential Papers
        </div>
        <div style={{ display: 'flex', gap: 4 }}>
          {SORT_OPTIONS.map(o => (
            <button
              key={o.value}
              onClick={() => setSort(o.value)}
              style={{
                padding: '4px 10px', fontSize: 10,
                background: sort === o.value ? 'var(--blue-dim)' : 'transparent',
                border: `1px solid ${sort === o.value ? 'var(--blue)' : 'var(--border)'}`,
                borderRadius: 4,
                color: sort === o.value ? 'var(--blue)' : 'var(--ink3)',
                cursor: 'pointer',
                transition: 'all var(--transition)',
              }}
            >{o.label}</button>
          ))}
        </div>
      </div>

      {loading ? (
        Array.from({ length: 5 }).map((_, i) => (
          <div key={i} className="skeleton" style={{ height: 76, borderRadius: 8, marginBottom: 8 }} />
        ))
      ) : sorted.map((paper, i) => (
        <PaperCard
          key={paper.id}
          paper={paper}
          rank={i + 1}
          expanded={selectedPaper?.id === paper.id}
          onClick={() => setSelectedPaper(selectedPaper?.id === paper.id ? null : paper)}
          style={{ animationDelay: `${i * 0.04}s` }}
        />
      ))}
    </div>
  )
}

function PaperCard({ paper, rank, expanded, onClick, style }) {
  const catColor = CATEGORY_COLORS[paper.category] ?? '#3b82f6'

  return (
    <div
      onClick={onClick}
      style={{
        background: expanded ? 'var(--bg3)' : 'var(--bg2)',
        border: `1px solid ${expanded ? 'rgba(59,130,246,0.3)' : 'var(--border)'}`,
        borderRadius: 'var(--radius-md)',
        padding: '12px 14px',
        marginBottom: 8,
        cursor: 'pointer',
        transition: 'all var(--transition)',
        animation: 'fadeUp 0.4s ease both',
        ...style,
      }}
      onMouseEnter={e => { if (!expanded) e.currentTarget.style.borderColor = 'var(--border2)' }}
      onMouseLeave={e => { if (!expanded) e.currentTarget.style.borderColor = 'var(--border)' }}
    >
      <div style={{ display: 'flex', gap: 10, alignItems: 'flex-start' }}>
        {/* Rank */}
        <span style={{
          fontSize: 10, color: 'var(--ink3)', minWidth: 18,
          fontFamily: 'var(--font-display)', fontStyle: 'italic',
          paddingTop: 2,
        }}>{rank}</span>

        {/* Main content */}
        <div style={{ flex: 1, minWidth: 0 }}>
          <div style={{
            fontFamily: 'var(--font-display)',
            fontSize: 13,
            color: 'var(--ink)',
            lineHeight: 1.4,
            marginBottom: 4,
          }}>{paper.title}</div>

          <div style={{ display: 'flex', gap: 10, flexWrap: 'wrap', fontSize: 10, color: 'var(--ink3)', marginBottom: 6 }}>
            <span>{paper.authors}</span>
            <span>{paper.venue} {paper.year}</span>
            <span>{paper.citations?.toLocaleString()} citations</span>
          </div>

          {/* Influence bar */}
          <div style={{ height: 3, background: 'var(--bg4)', borderRadius: 2, overflow: 'hidden' }}>
            <div style={{
              height: '100%',
              width: `${paper.pagerank}%`,
              background: `linear-gradient(90deg, ${catColor}88, ${catColor})`,
              borderRadius: 2,
            }} />
          </div>

          {/* Expanded abstract */}
          {expanded && paper.abstract && (
            <div style={{
              marginTop: 10,
              fontSize: 11,
              color: 'var(--ink2)',
              lineHeight: 1.6,
              borderTop: '1px solid var(--border)',
              paddingTop: 10,
            }}>{paper.abstract}</div>
          )}
        </div>

        {/* Right side */}
        <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: 5, flexShrink: 0 }}>
          <span style={{
            padding: '2px 7px',
            borderRadius: 3,
            fontSize: 9,
            letterSpacing: '0.3px',
            background: catColor + '18',
            color: catColor,
            border: `1px solid ${catColor}33`,
          }}>{paper.category}</span>
          <span style={{ fontSize: 9, color: 'var(--ink3)' }}>PR {paper.pagerank}</span>
          {expanded
            ? <ChevronUp size={13} color="var(--ink3)" />
            : <ChevronDown size={13} color="var(--ink3)" />
          }
        </div>
      </div>
    </div>
  )
}
