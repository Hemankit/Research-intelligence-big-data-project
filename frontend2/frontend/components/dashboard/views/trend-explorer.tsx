"use client"

import { useEffect, useState } from "react"
import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts"
import { cn } from "@/lib/utils"
import { fetchTrends } from "@/lib/api"

const categories = ["All categories", "cs.LG", "cs.CL", "cs.CV", "cs.AI", "cs.IR"]

export function TrendExplorerView() {
  const [activeCategory, setActiveCategory] = useState("All categories")
  const [trendData, setTrendData] = useState<any[]>([])
  const [citationData, setCitationData] = useState<any[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    const categoryFilter = activeCategory === "All categories" ? undefined : activeCategory
    fetchTrends(categoryFilter)
      .then((data) => {
        const trends: any[] = data.trends || []

        // Build monthly chart: group by year_month, sum paper_count
        const monthMap: Record<string, number> = {}
        trends.forEach((t: any) => {
          if (t.year_month && !t.topic_cluster) {
            monthMap[t.year_month] = (monthMap[t.year_month] || 0) + (t.paper_count || 0)
          }
        })
        const chartData = Object.entries(monthMap)
          .sort(([a], [b]) => a.localeCompare(b))
          .map(([month, count]) => ({ month, count }))
        setTrendData(chartData)

        // Build citation by category: group by primary_category, avg citation count
        const catMap: Record<string, { total: number; rows: number }> = {}
        trends.forEach((t: any) => {
          if (t.primary_category && t.avg_citation_count != null) {
            if (!catMap[t.primary_category]) catMap[t.primary_category] = { total: 0, rows: 0 }
            catMap[t.primary_category].total += t.avg_citation_count
            catMap[t.primary_category].rows += 1
          }
        })
        const citData = Object.entries(catMap)
          .map(([category, v]) => ({ category, count: parseFloat((v.total / v.rows).toFixed(2)) }))
          .sort((a, b) => b.count - a.count)
          .slice(0, 5)
        setCitationData(citData)
      })
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [activeCategory])

  const maxCitation = citationData.length > 0 ? Math.max(...citationData.map((d) => d.count)) : 1

  return (
    <div className="space-y-6">
      <div className="flex flex-wrap gap-2">
        {categories.map((category) => (
          <button
            key={category}
            onClick={() => setActiveCategory(category)}
            className={cn(
              "rounded-full px-4 py-1.5 text-sm font-medium transition-colors",
              activeCategory === category
                ? "bg-primary text-primary-foreground"
                : "bg-secondary text-muted-foreground hover:bg-secondary/80 hover:text-foreground"
            )}
          >
            {category}
          </button>
        ))}
      </div>

      <div className="rounded-lg bg-card p-5">
        <h3 className="mb-4 text-lg font-semibold text-card-foreground">Paper Count Over Time</h3>
        {loading ? (
          <div className="flex h-[300px] items-center justify-center">
            <p className="text-muted-foreground text-sm">Loading trends...</p>
          </div>
        ) : trendData.length > 0 ? (
          <ResponsiveContainer width="100%" height={300}>
            <AreaChart data={trendData}>
              <defs>
                <linearGradient id="colorCount" x1="0" y1="0" x2="0" y2="1">
                  <stop offset="5%" stopColor="#378ADD" stopOpacity={0.3} />
                  <stop offset="95%" stopColor="#378ADD" stopOpacity={0} />
                </linearGradient>
              </defs>
              <CartesianGrid strokeDasharray="3 3" stroke="#2D3139" vertical={false} />
              <XAxis dataKey="month" stroke="#9CA3AF" fontSize={12} tickLine={false} axisLine={false} />
              <YAxis stroke="#9CA3AF" fontSize={12} tickLine={false} axisLine={false} />
              <Tooltip
                contentStyle={{
                  backgroundColor: "#1C1F26",
                  border: "1px solid #2D3139",
                  borderRadius: "8px",
                  color: "#ffffff",
                }}
              />
              <Area
                type="monotone"
                dataKey="count"
                stroke="#378ADD"
                strokeWidth={2}
                fillOpacity={1}
                fill="url(#colorCount)"
              />
            </AreaChart>
          </ResponsiveContainer>
        ) : (
          <p className="py-8 text-center text-sm text-muted-foreground">
            No trend data — run spark_trends.py to populate.
          </p>
        )}
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="rounded-lg bg-card p-5">
          <h3 className="mb-4 text-lg font-semibold text-card-foreground">Avg Citation Count by Category (from arXiv papers)</h3>
          {citationData.length > 0 ? (
            <ul className="space-y-4">
              {citationData.map((item) => (
                <li key={item.category} className="space-y-1.5">
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium text-card-foreground">{item.category}</span>
                    <span className="text-sm text-muted-foreground">{item.count}</span>
                  </div>
                  <div className="h-2 rounded-full bg-secondary">
                    <div
                      className="h-full rounded-full bg-primary"
                      style={{ width: `${(item.count / maxCitation) * 100}%` }}
                    />
                  </div>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-sm text-muted-foreground">No citation data available.</p>
          )}
        </div>

        <div className="rounded-lg bg-card p-5">
          <h3 className="mb-4 text-lg font-semibold text-card-foreground">Top Categories by Volume (from arXiv papers)</h3>
          {citationData.length > 0 ? (
            <ul className="space-y-3">
              {citationData.map((item) => (
                <li key={item.category} className="flex items-center justify-between">
                  <span className="text-sm font-medium text-card-foreground">{item.category}</span>
                  <span className="text-sm font-medium text-primary">{item.count.toFixed(1)} Avg citations</span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-sm text-muted-foreground">No data available.</p>
          )}
        </div>
      </div>
    </div>
  )
}
