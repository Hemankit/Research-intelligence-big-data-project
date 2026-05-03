"use client"

import { useEffect, useState, useMemo } from "react"
import { fetchLandscape } from "@/lib/api"

const CLUSTER_COLORS = [
  "#378ADD", "#1D9E75", "#7F77DD", "#D85A30",
  "#BA7517", "#D4537E", "#5DCAA5", "#EF9F27",
]

export function TopicLandscapeView() {
  const [points, setPoints] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [activeCluster, setActiveCluster] = useState<number | null>(null)
  const [searchQuery, setSearchQuery] = useState("")
  const [pointLimit, setPointLimit] = useState(3000)
  const [hoveredPoint, setHoveredPoint] = useState<{ x: number; y: number; title: string } | null>(null)
  const [matchedIds, setMatchedIds] = useState<Set<string> | null>(null)
  const [searching, setSearching] = useState(false)

  useEffect(() => {
  setLoading(true)
  fetchLandscape(pointLimit)
    .then((data) => setPoints(data.points || []))
    .catch(console.error)
    .finally(() => setLoading(false))
}, [pointLimit])

  // ES semantic search with debounce
  useEffect(() => {
    if (!searchQuery.trim()) { setMatchedIds(null); return }
    const timer = setTimeout(() => {
      setSearching(true)
      const BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000"
      fetch(`${BASE}/api/papers/search?q=${encodeURIComponent(searchQuery)}&size=500`)
        .then(r => r.json())
        .then(d => {
          const ids = new Set<string>((d.papers ?? []).map((p: any) => p.paper_id))
          setMatchedIds(ids)
        })
        .catch(() => setMatchedIds(null))
        .finally(() => setSearching(false))
    }, 600)
    return () => clearTimeout(timer)
  }, [searchQuery])

  // Normalize umap_x and umap_y to SVG viewBox (0-700 x 0-450)
  const normalizedPoints = useMemo(() => {
    if (points.length === 0) return []
    const xs = points.map((p) => p.umap_x).filter((v) => v != null)
    const ys = points.map((p) => p.umap_y).filter((v) => v != null)
    const minX = Math.min(...xs), maxX = Math.max(...xs)
    const minY = Math.min(...ys), maxY = Math.max(...ys)
    const rangeX = maxX - minX || 1
    const rangeY = maxY - minY || 1
    return points
      .filter((p) => p.umap_x != null && p.umap_y != null)
      .map((p) => ({
        ...p,
        svgX: ((p.umap_x - minX) / rangeX) * 660 + 20,
        svgY: ((p.umap_y - minY) / rangeY) * 410 + 20,
      }))
  }, [points])

  // Build legend from unique clusters
  const clusters = useMemo(() => {
    const seen = new Map<number, string>()
    points.forEach((p) => {
      if (p.topic_cluster_id != null && p.topic_cluster_id !== -1 && !seen.has(p.topic_cluster_id)) {
        seen.set(p.topic_cluster_id, p.topic_cluster || `Cluster ${p.topic_cluster_id}`)
      }
    })
    return Array.from(seen.entries())
      .slice(0, 8)
      .map(([id, name]) => ({ id, name, color: CLUSTER_COLORS[id % CLUSTER_COLORS.length] }))
  }, [points])

  const visiblePoints = useMemo(() => {
    let filtered = activeCluster === null ? normalizedPoints
      : normalizedPoints.filter(p => p.topic_cluster_id === activeCluster)
    if (matchedIds !== null) {
      filtered = filtered.filter(p => matchedIds.has(p.paper_id))
    }
    return filtered
  }, [normalizedPoints, activeCluster, matchedIds])

  const getColor = (clusterId: number) => {
    if (clusterId === -1) return "#4B5563"
    return CLUSTER_COLORS[clusterId % CLUSTER_COLORS.length]
  }

  

  return (
    <div className="space-y-6">
      <div className="rounded-lg bg-card p-5">
        <h3 className="mb-4 text-lg font-semibold text-card-foreground">Topic Clusters (UMAP Projection)</h3>

        {loading ? (
          <div className="flex h-[450px] items-center justify-center">
            <p className="text-muted-foreground text-sm">Loading landscape data...</p>
          </div>
        ) : normalizedPoints.length === 0 ? (
          <div className="flex h-[450px] items-center justify-center">
            <p className="text-sm text-muted-foreground">
              No UMAP data yet — run BERTopic then spark_consolidate.py first.
            </p>
          </div>
        ) : (
          <div className="relative">
            {pointLimit > 5000 && (
              <p className="text-xs text-[#BA7517] mb-2">
                ⚠ High point counts may slow rendering
              </p>
            )}
            <div className="mb-3 flex items-center gap-4">
              <span className="text-xs text-muted-foreground shrink-0">Points:</span>
              <input
                type="range"
                min={500}
                max={20000}
                step={500}
                value={pointLimit}
                onChange={(e) => setPointLimit(Number(e.target.value))}
                className="flex-1 accent-primary"
              />
              <span className="text-xs font-medium text-primary w-16 text-right">
                {pointLimit.toLocaleString()}
              </span>
            </div>
            <div className="mb-3 relative">
              <input
                type="text"
                placeholder="Search papers semantically (e.g. transformer attention mechanism)..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                className="w-full rounded-lg bg-secondary px-4 py-2 text-sm text-card-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary pr-16"
              />
              <div className="absolute right-3 top-1/2 -translate-y-1/2 flex items-center gap-2">
                {searching && <span className="text-xs text-muted-foreground animate-pulse">Searching...</span>}
                {searchQuery && !searching && (
                  <button
                    onClick={() => { setSearchQuery(""); setMatchedIds(null); }}
                    className="text-xs text-muted-foreground hover:text-foreground"
                  >
                    ✕
                  </button>
                )}
              </div>
              {matchedIds !== null && (
                <p className="mt-1 text-xs text-primary">
                  {matchedIds.size} papers matched — showing their positions on the map
                </p>
              )}
            </div>
            <svg viewBox="0 0 700 450" className="h-[450px] w-full">
              {visiblePoints.map((point, index) => (
                <circle
                  key={point.paper_id ?? index}
                  cx={point.svgX}
                  cy={point.svgY}
                  r={matchedIds !== null ? 7 : 5}
                  fill={getColor(point.topic_cluster_id ?? -1)}
                  opacity={matchedIds !== null ? 1.0 : 0.7}
                  className="cursor-pointer transition-all hover:opacity-100"
                  onMouseEnter={() =>
                    setHoveredPoint({ x: point.svgX, y: point.svgY, title: point.title || point.paper_id })
                  }
                  onMouseLeave={() => setHoveredPoint(null)}
                />
              ))}
            </svg>
            {hoveredPoint && (
              <div
                className="pointer-events-none absolute z-10 max-w-[220px] rounded-lg bg-popover px-3 py-2 text-sm text-popover-foreground shadow-lg border border-border"
                style={{
                  left: `${(hoveredPoint.x / 700) * 100}%`,
                  top: `${(hoveredPoint.y / 450) * 100}%`,
                  transform: "translate(-50%, -120%)",
                }}
              >
                {hoveredPoint.title}
              </div>
            )}
          </div>
        )}

        {clusters.length > 0 && (
          <div className="mt-4 flex flex-wrap items-center justify-center gap-4">
            {activeCluster !== null && (
              <button
                onClick={() => setActiveCluster(null)}
                className="text-xs text-primary hover:underline w-full text-center mb-2"
              >
                ← Show all clusters
              </button>
            )}
            {clusters.map((cluster) => (
              <div
                key={cluster.id}
                onClick={() => setActiveCluster(activeCluster === cluster.id ? null : cluster.id)}
                className={`flex items-center gap-2 cursor-pointer rounded px-2 py-1 transition-all ${
                  activeCluster === cluster.id
                    ? "bg-secondary ring-1 ring-primary"
                    : activeCluster !== null
                    ? "opacity-40 hover:opacity-100"
                    : "hover:bg-secondary/50"
                }`}
              >
                <span className="h-3 w-3 rounded-full shrink-0" style={{ backgroundColor: cluster.color }} />
                <span className="text-sm text-muted-foreground">{cluster.name}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="rounded-lg bg-card p-5">
        <p className="text-sm text-muted-foreground">
          Each point is a paper positioned by its UMAP coordinates from BERTopic. Hover a point to see the title. Click a cluster in the legend to filter.
          {normalizedPoints.length > 0 && (
            <span className="ml-1 text-muted-foreground">Showing {visiblePoints.length.toLocaleString()} of {normalizedPoints.length.toLocaleString()} papers.</span>
          )}
        </p>
      </div>
    </div>
  )
}
