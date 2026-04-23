import { useRef, useEffect } from 'react'
import { useTrendingTopics, useTrendingEntities, usePipelineStatus } from '../hooks/useData'

export default function RightPanel() {
  return (
    <aside style={{
      width: 260,
      background: 'var(--bg2)',
      borderLeft: '1px solid var(--border)',
      padding: '16px 14px',
      display: 'flex',
      flexDirection: 'column',
      gap: 20,
      overflowY: 'auto',
      flexShrink: 0,
    }}>
      <TrendingTopics />
      <EntityExplorer />
      <PipelineStatus />
      <CitationGraphMini />
    </aside>
  )
}

/* ── Trending ───────────────────────────────────────────────── */
function TrendingTopics() {
  const { data, loading } = useTrendingTopics()

  return (
    <Section title="Trending Right Now">
      {loading && <Skeleton rows={6} />}
      {data?.map(t => (
        <div key={t.name} style={{ display: 'flex', alignItems: 'center', justifyContent: 'space-between', padding: '6px 0', borderBottom: '1px solid var(--border)' }}>
          <span style={{ fontSize: 11, color: 'var(--ink2)' }}>{t.name}</span>
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <div style={{ width: 56, height: 3, background: 'var(--bg4)', borderRadius: 2 }}>
              <div style={{ width: `${t.pct}%`, height: '100%', background: t.color, borderRadius: 2 }} />
            </div>
            <span style={{ fontSize: 9, color: 'var(--ink3)', minWidth: 26, textAlign: 'right' }}>{t.pct}%</span>
          </div>
        </div>
      ))}
    </Section>
  )
}

/* ── Entity Explorer ────────────────────────────────────────── */
const ENTITY_PALETTES = {
  methods:  { bg: 'rgba(59,130,246,0.1)',  color: '#3b82f6',  dot: '#3b82f6'  },
  datasets: { bg: 'rgba(16,185,129,0.1)',  color: '#10b981',  dot: '#10b981'  },
  tasks:    { bg: 'rgba(245,158,11,0.1)',  color: '#f59e0b',  dot: '#f59e0b'  },
}

function EntityExplorer() {
  const { data, loading } = useTrendingEntities()

  return (
    <Section title="Entity Explorer">
      {loading && <Skeleton rows={3} />}
      {data && (
        <>
          <EntityRow label="Methods"  items={data.methods}  palette={ENTITY_PALETTES.methods} />
          <EntityRow label="Datasets" items={data.datasets} palette={ENTITY_PALETTES.datasets} style={{ marginTop: 10 }} />
          <EntityRow label="Tasks"    items={data.tasks}    palette={ENTITY_PALETTES.tasks}    style={{ marginTop: 10 }} />
        </>
      )}
    </Section>
  )
}

function EntityRow({ label, items = [], palette, style }) {
  return (
    <div style={style}>
      <div style={{ fontSize: 9, letterSpacing: '1px', textTransform: 'uppercase', color: 'var(--ink3)', marginBottom: 5 }}>
        {label}
      </div>
      <div style={{ display: 'flex', flexWrap: 'wrap', gap: 4 }}>
        {items.map(item => (
          <span key={item.name ?? item} style={{
            display: 'inline-flex', alignItems: 'center', gap: 4,
            padding: '3px 7px',
            background: palette.bg,
            border: `1px solid ${palette.color}22`,
            borderRadius: 4,
            fontSize: 10,
            color: palette.color,
            cursor: 'pointer',
          }}>
            <span style={{ width: 4, height: 4, borderRadius: '50%', background: palette.dot }} />
            {item.name ?? item}
          </span>
        ))}
      </div>
    </div>
  )
}

/* ── Pipeline Status ────────────────────────────────────────── */
const STATUS_STYLE = {
  ok:      { bg: 'rgba(16,185,129,0.1)', color: '#10b981', dot: '#10b981', label: 'done'    },
  running: { bg: 'rgba(245,158,11,0.1)', color: '#f59e0b', dot: '#f59e0b', label: 'running' },
  queued:  { bg: 'var(--bg4)',           color: 'var(--ink3)', dot: 'var(--ink3)', label: 'queued' },
}

function PipelineStatus() {
  const { data, loading } = usePipelineStatus()

  return (
    <Section title="Pipeline Status">
      {loading && <Skeleton rows={5} />}
      {data?.map(step => {
        const s = STATUS_STYLE[step.status] ?? STATUS_STYLE.queued
        return (
          <div key={step.label} style={{ display: 'flex', alignItems: 'center', gap: 8, padding: '5px 0', borderBottom: '1px solid var(--border)' }}>
            <span style={{ width: 5, height: 5, borderRadius: '50%', background: s.dot, flexShrink: 0,
              animation: step.status === 'running' ? 'pulse-dot 1.5s infinite' : 'none' }} />
            <span style={{ flex: 1, fontSize: 10, color: 'var(--ink2)' }}>{step.label}</span>
            <span style={{ fontSize: 9, padding: '1px 6px', borderRadius: 3, background: s.bg, color: s.color }}>
              {s.label}
            </span>
          </div>
        )
      })}
    </Section>
  )
}

/* ── Citation Graph Mini (canvas) ───────────────────────────── */
function CitationGraphMini() {
  const canvasRef = useRef(null)

  useEffect(() => {
    const canvas = canvasRef.current
    if (!canvas) return
    const ctx = canvas.getContext('2d')
    const W = canvas.width, H = canvas.height
    ctx.clearRect(0, 0, W, H)

    const rng = (s) => { let x = Math.sin(s * 1337) * 10000; return x - Math.floor(x) }
    const COLORS = ['#3b82f6','#10b981','#2dd4bf','#a78bfa','#f59e0b']
    const nodes = Array.from({ length: 40 }, (_, i) => ({
      x: 12 + rng(i * 2) * (W - 24),
      y: 12 + rng(i * 3 + 1) * (H - 24),
      r: 2 + rng(i * 5) * 5,
      c: COLORS[Math.floor(rng(i * 7) * COLORS.length)],
    }))

    nodes.forEach((a, i) => {
      nodes.forEach((b, j) => {
        if (i >= j) return
        const d = Math.hypot(a.x - b.x, a.y - b.y)
        if (d < 70) {
          ctx.strokeStyle = `rgba(255,255,255,${0.04 * (1 - d / 70)})`
          ctx.lineWidth = 0.5
          ctx.beginPath(); ctx.moveTo(a.x, a.y); ctx.lineTo(b.x, b.y); ctx.stroke()
        }
      })
    })

    nodes.forEach(n => {
      ctx.fillStyle = n.c + '80'
      ctx.beginPath(); ctx.arc(n.x, n.y, n.r, 0, Math.PI * 2); ctx.fill()
      ctx.strokeStyle = n.c
      ctx.lineWidth = 0.5
      ctx.stroke()
    })
  }, [])

  return (
    <Section title="Citation Graph Preview">
      <div style={{ background: 'var(--bg)', borderRadius: 'var(--radius-md)', overflow: 'hidden', marginBottom: 6 }}>
        <canvas ref={canvasRef} width={228} height={140} style={{ width: '100%', height: 140, display: 'block' }} />
      </div>
      <div style={{ fontSize: 9, color: 'var(--ink3)', textAlign: 'center' }}>
        Top 40 nodes by PageRank · GraphX / S2ORC
      </div>
    </Section>
  )
}

/* ── Helpers ────────────────────────────────────────────────── */
function Section({ title, children }) {
  return (
    <div>
      <div style={{
        fontSize: 9, letterSpacing: '1.5px', textTransform: 'uppercase',
        color: 'var(--ink3)', marginBottom: 8,
        paddingBottom: 5, borderBottom: '1px solid var(--border)',
      }}>{title}</div>
      {children}
    </div>
  )
}

function Skeleton({ rows = 4 }) {
  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 6 }}>
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="skeleton" style={{ height: 20, width: `${70 + (i % 3) * 10}%`, borderRadius: 3 }} />
      ))}
    </div>
  )
}
