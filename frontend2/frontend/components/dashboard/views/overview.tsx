"use client"

import { useEffect, useState } from "react"
import { FileText, Share2, Layers, Cpu } from "lucide-react"
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts"
import { StatCard } from "../stat-card"
import { fetchStats, fetchTopPapers, fetchTrends } from "@/lib/api"
import { NLQueryBar } from "./nl-query"

const categoryColors: Record<string, string> = {
  "cs.CL": "bg-primary/20 text-primary",
  "cs.CV": "bg-[#7F77DD]/20 text-[#7F77DD]",
  "cs.LG": "bg-[#1D9E75]/20 text-[#1D9E75]",
  "cs.AI": "bg-[#D85A30]/20 text-[#D85A30]",
  "cs.IR": "bg-[#BA7517]/20 text-[#BA7517]",
}

export function OverviewView() {
  const [stats, setStats] = useState<any>(null)
  const [topPapers, setTopPapers] = useState<any[]>([])
  const [monthlyData, setMonthlyData] = useState<any[]>([])
  const [recentPapers, setRecentPapers] = useState<any[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    Promise.all([
      fetchStats(),
      fetchTopPapers(4),
      fetchTrends(),
      // recent papers: fetch latest 5 sorted by ingested date
      import("@/lib/api").then(m => m.fetchPapers({ limit: 5, sort_by: "submitted_date", offset: 0 })),
    ])
      .then(([statsData, topData, trendsData, recentData]) => {
        setStats(statsData)
        setTopPapers(topData.papers || [])

        // Aggregate trend rows by year_month, sum paper_count
        const grouped: Record<string, number> = {}
        ;(trendsData.trends || []).forEach((t: any) => {
          if (t.year_month) {
            grouped[t.year_month] = (grouped[t.year_month] || 0) + (t.paper_count || 0)
          }
        })
        const chartData = Object.entries(grouped)
          .sort(([a], [b]) => a.localeCompare(b))
          .slice(-12)
          .map(([month, papers]) => ({ month: month.slice(2), papers }))
        setMonthlyData(chartData)

        setRecentPapers(recentData.papers || [])
      })
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [])

  const totalPapers = stats?.papers?.total_papers?.toLocaleString() ?? "—"
  const scoredPapers = stats?.pagerank?.scored_papers?.toLocaleString() ?? "—"
  const topicCount = stats?.trends?.topic_count?.toString() ?? "—"
  const totalEdges = stats?.citations?.total_edges?.toLocaleString() ?? "—"

  if (loading) {
    return (
      <div className="flex h-[400px] items-center justify-center rounded-lg bg-card">
        <p className="text-muted-foreground">Loading overview...</p>
      </div>
    )
  }

  return (
    <div className="space-y-6">
      <NLQueryBar />
      <div className="grid grid-cols-4 gap-4">
        <StatCard title="Total Papers" value={totalPapers} icon={FileText} />
        <StatCard title="Scored Papers" value={scoredPapers} icon={Share2} />
        <StatCard title="Topic Clusters" value={topicCount} icon={Layers} />
        <StatCard title="Citation Edges" value={totalEdges} icon={Cpu} />
      </div>

      <div className="rounded-lg bg-card p-5">
        <h3 className="mb-4 text-lg font-semibold text-card-foreground">Papers Ingested by Month</h3>
        {monthlyData.length > 0 ? (
          <ResponsiveContainer width="100%" height={280}>
            <BarChart data={monthlyData}>
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
              <Bar dataKey="papers" fill="#378ADD" radius={[4, 4, 0, 0]} />
            </BarChart>
          </ResponsiveContainer>
        ) : (
          <p className="py-8 text-center text-sm text-muted-foreground">No trend data yet — run spark_trends.py first.</p>
        )}
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="rounded-lg bg-card p-5">
          <h3 className="mb-4 text-lg font-semibold text-card-foreground">Top Papers by PageRank</h3>
          {topPapers.length > 0 ? (
            <ul className="space-y-4">
              {topPapers.map((paper: any, i: number) => (
                <li key={paper.paper_id} className="flex items-start gap-3">
                  <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-primary/20 text-xs font-bold text-primary">
                    {i + 1}
                  </span>
                  <div className="flex-1 space-y-1">
                    <p className="text-sm font-medium text-card-foreground leading-tight">{paper.title}</p>
                    <div className="flex items-center gap-2">
                      <span className={`rounded px-1.5 py-0.5 text-xs font-medium ${categoryColors[paper.primary_category] ?? "bg-secondary text-muted-foreground"}`}>
                        {paper.primary_category}
                      </span>
                      <span className="text-xs text-muted-foreground">
                        {paper.submitted_date ? paper.submitted_date.slice(0, 4) : ""}
                      </span>
                    </div>
                    <div className="flex items-center gap-2">
                      <div className="h-1.5 flex-1 rounded-full bg-secondary">
                        <div
                          className="h-full rounded-full bg-primary"
                          style={{ width: `${(paper.pagerank_score ?? 0) * 100}%` }}
                        />
                      </div>
                      <span className="text-xs font-medium text-primary">
                        {paper.pagerank_score != null ? paper.pagerank_score.toFixed(2) : "—"}
                      </span>
                    </div>
                  </div>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-sm text-muted-foreground">No papers found.</p>
          )}
        </div>

        <div className="rounded-lg bg-card p-5">
          <h3 className="mb-4 text-lg font-semibold text-card-foreground">Recently Ingested</h3>
          {recentPapers.length > 0 ? (
            <ul className="space-y-3">
              {recentPapers.map((paper: any, index: number) => (
                <li key={paper.paper_id ?? index} className="flex items-center gap-3">
                  <span className="h-2 w-2 shrink-0 rounded-full bg-[#1D9E75]" />
                  <p className="flex-1 truncate text-sm text-card-foreground">{paper.title}</p>
                  <span className="shrink-0 text-xs text-muted-foreground">
                    {paper.submitted_date ? paper.submitted_date.slice(0, 7) : ""}
                  </span>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-sm text-muted-foreground">No recent papers found.</p>
          )}
        </div>
      </div>
    </div>
  )
}
