import { useRef, useEffect, useState } from 'react'
import { useLandscape } from '../hooks/useData'
import { useStore } from '../hooks/useStore'

export default function LandscapeMap() {
  const { filters, selectedCluster, setSelectedCluster } = useStore()
  const { data, loading } = useLandscape(filters)
  const canvasRef = useRef(null)
  const [hovered, setHovered] = useState(null)

  useEffect(() => {
    if (!data || !canvasRef.current) return
    const canvas = canvasRef.current
    const ctx = canvas.getContext('2d')
    const W = canvas.width
    const H = canvas.height
    ctx.clearRect(0, 0, W, H)

    // Background grid
    ctx.strokeStyle = 'rgba(255,255,255,0.03)'
    ctx.lineWidth = 0.5
    for (let x = 0; x < W; x += 40) { ctx.beginPath(); ctx.moveTo(x,0); ctx.lineTo(x,H); ctx.stroke() }
    for (let y = 0; y < H; y += 40) { ctx.beginPath(); ctx.moveTo(0,y); ctx.lineTo(W,y); ctx.stroke() }

    // Cluster glows
    data.clusters.forEach(cl => {
      const cx = cl.cx * W, cy = cl.cy * H
      const grad = ctx.createRadialGradient(cx, cy, 0, cx, cy, 80)
      grad.addColorStop(0, cl.color + '18')
      grad.addColorStop(1, 'transparent')
      ctx.fillStyle = grad
      ctx.beginPath(); ctx.arc(cx, cy, 80, 0, Math.PI * 2); ctx.fill()
    })

    // Points
    data.points.forEach(pt => {
      const x = pt.x * W, y = pt.y * H
      const r = 1.5 + pt.pagerank * 4
      const dimmed = selectedCluster !== null && pt.cluster_id !== selectedCluster
      ctx.globalAlpha = dimmed ? 0.15 : 0.55 + pt.pagerank * 0.4
      ctx.fillStyle = pt.color
      ctx.beginPath(); ctx.arc(x, y, r, 0, Math.PI * 2); ctx.fill()
    })
    ctx.globalAlpha = 1

    // Cluster labels
    data.clusters.forEach(cl => {
      const cx = cl.cx * W, cy = cl.cy * H
      const dimmed = selectedCluster !== null && cl.id !== selectedCluster
      ctx.globalAlpha = dimmed ? 0.2 : 1

      ctx.font = '500 11px DM Mono, monospace'
      ctx.textAlign = 'center'
      ctx.fillStyle = 'rgba(240,237,232,0.9)'
      ctx.fillText(cl.name, cx, cy - 20)

      ctx.font = '9px DM Mono, monospace'
      ctx.fillStyle = 'rgba(157,163,176,0.7)'
      ctx.fillText(`${cl.count} papers`, cx, cy - 8)
    })
    ctx.globalAlpha = 1
  }, [data, selectedCluster])

  const handleClick = (e) => {
    if (!data) return
    const canvas = canvasRef.current
    const rect = canvas.getBoundingClientRect()
    const mx = (e.clientX - rect.left) / rect.width
    const my = (e.clientY - rect.top) / rect.height
    let nearest = null, minDist = Infinity
    data.clusters.forEach(cl => {
      const d = Math.hypot(cl.cx - mx, cl.cy - my)
      if (d < minDist) { minDist = d; nearest = cl }
    })
    if (minDist < 0.15) {
      setSelectedCluster(selectedCluster === nearest.id ? null : nearest.id)
    }
  }

  return (
    <div style={{
      background: 'var(--bg)',
      border: '1px solid var(--border)',
      borderRadius: 'var(--radius-lg)',
      padding: 16,
    }}>
      <div style={{ marginBottom: 12, display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start' }}>
        <div>
          <div style={{ fontFamily: 'var(--font-display)', fontSize: 15 }}>Research Landscape</div>
          <div style={{ fontSize: 10, color: 'var(--ink3)', marginTop: 2 }}>
            UMAP projection · BERTopic clusters · point size = PageRank · click cluster to filter
          </div>
        </div>
        {selectedCluster !== null && (
          <button
            onClick={() => setSelectedCluster(null)}
            style={{ fontSize: 10, color: 'var(--blue)', background: 'none', border: 'none', cursor: 'pointer' }}
          >Clear filter ×</button>
        )}
      </div>

      {loading
        ? <div className="skeleton" style={{ height: 380, borderRadius: 8 }} />
        : (
          <canvas
            ref={canvasRef}
            width={760}
            height={380}
            onClick={handleClick}
            style={{ width: '100%', height: 380, display: 'block', cursor: 'crosshair', borderRadius: 6 }}
          />
        )
      }

      {data && (
        <div style={{ display: 'flex', gap: 12, marginTop: 10, flexWrap: 'wrap' }}>
          {data.clusters.map(cl => (
            <button
              key={cl.id}
              onClick={() => setSelectedCluster(selectedCluster === cl.id ? null : cl.id)}
              style={{
                display: 'flex', alignItems: 'center', gap: 5,
                fontSize: 10, color: selectedCluster === cl.id ? cl.color : 'var(--ink3)',
                background: 'none', border: 'none', cursor: 'pointer',
                opacity: selectedCluster !== null && selectedCluster !== cl.id ? 0.4 : 1,
                transition: 'all var(--transition)',
              }}
            >
              <span style={{ width: 7, height: 7, borderRadius: '50%', background: cl.color }} />
              {cl.name}
            </button>
          ))}
        </div>
      )}
    </div>
  )
}
