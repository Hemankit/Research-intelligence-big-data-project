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
  const [hoveredPoint, setHoveredPoint] = useState<{ x: number; y: number; title: string } | null>(null)

  useEffect(() => {
    fetchLandscape(3000)
      .then((data) => setPoints(data.points || []))
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [])

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
            <svg viewBox="0 0 700 450" className="h-[450px] w-full">
              {normalizedPoints.map((point, index) => (
                <circle
                  key={point.paper_id ?? index}
                  cx={point.svgX}
                  cy={point.svgY}
                  r={5}
                  fill={getColor(point.topic_cluster_id ?? -1)}
                  opacity={0.7}
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
            {clusters.map((cluster) => (
              <div key={cluster.id} className="flex items-center gap-2">
                <span className="h-3 w-3 rounded-full" style={{ backgroundColor: cluster.color }} />
                <span className="text-sm text-muted-foreground">{cluster.name}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      <div className="rounded-lg bg-card p-5">
        <p className="text-sm text-muted-foreground">
          Each point is a paper positioned by its UMAP coordinates from BERTopic. Hover a point to see the title.
          {normalizedPoints.length > 0 && (
            <span className="ml-1 text-muted-foreground">Showing {normalizedPoints.length.toLocaleString()} papers.</span>
          )}
        </p>
      </div>
    </div>
  )
}
