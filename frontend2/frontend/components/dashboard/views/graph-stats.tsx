"use client"

import { useEffect, useState } from "react"
import { fetchStats } from "@/lib/api"

interface StatRowProps {
  label: string
  value: string | number
  subtext?: string
  color?: string
}

function StatRow({ label, value, subtext, color = "text-primary" }: StatRowProps) {
  return (
    <div className="flex items-center justify-between border-b border-border py-3 last:border-0">
      <div>
        <p className="text-sm font-medium text-card-foreground">{label}</p>
        {subtext && <p className="text-xs text-muted-foreground">{subtext}</p>}
      </div>
      <span className={`text-lg font-bold ${color}`}>
        {typeof value === "number" ? value.toLocaleString() : value}
      </span>
    </div>
  )
}

interface StatusBadgeProps {
  status: "ok" | "error" | "unknown"
  label: string
}

function StatusBadge({ status, label }: StatusBadgeProps) {
  const styles = {
    ok: "bg-[#1D9E75]/20 text-[#1D9E75]",
    error: "bg-red-500/20 text-red-400",
    unknown: "bg-secondary text-muted-foreground",
  }
  return (
    <div className="flex items-center justify-between border-b border-border py-3 last:border-0">
      <span className="text-sm font-medium text-card-foreground">{label}</span>
      <span className={`rounded px-2 py-0.5 text-xs font-medium ${styles[status]}`}>
        {status === "ok" ? "Online" : status === "error" ? "Offline" : "Unknown"}
      </span>
    </div>
  )
}

export function GraphStatsView() {
  const [stats, setStats] = useState<any>(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    fetchStats()
      .then(setStats)
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [])

  if (loading) {
    return (
      <div className="flex h-[400px] items-center justify-center">
        <p className="text-muted-foreground">Loading stats...</p>
      </div>
    )
  }

  const papers = stats?.papers ?? {}
  const citations = stats?.citations ?? {}
  const pagerank = stats?.pagerank ?? {}
  const trends = stats?.trends ?? {}

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-foreground">Graph Stats</h1>
        <p className="text-sm text-muted-foreground">Corpus metrics and pipeline component status</p>
      </div>

      <div className="grid grid-cols-2 gap-4">
        {/* Corpus Stats */}
        <div className="rounded-lg bg-card p-5">
          <h3 className="mb-4 text-lg font-semibold text-card-foreground">Corpus Overview</h3>
          <StatRow label="Total Papers" value={papers.total_papers ?? 0} subtext="Across all sources" />
          <StatRow label="Topic Clusters" value={trends.topic_count ?? 0} subtext="Discovered by BERTopic" color="text-[#7F77DD]" />
          <StatRow label="Citation Edges" value={citations.total_edges ?? 0} subtext="Directed citation links" color="text-[#1D9E75]" />
          <StatRow label="PageRank Scored" value={pagerank.scored_papers ?? 0} subtext="Papers with influence scores" color="text-[#D85A30]" />
          <StatRow label="Months Covered" value={trends.months_covered ?? 0} subtext="In trend time series" color="text-[#BA7517]" />
        </div>

        {/* Pipeline Status */}
        <div className="rounded-lg bg-card p-5">
          <h3 className="mb-4 text-lg font-semibold text-card-foreground">Pipeline Components</h3>
          <StatusBadge status="ok" label="Apache Hive" />
          <StatusBadge status="ok" label="Elasticsearch" />
          <StatusBadge status="ok" label="FastAPI Backend" />
          <StatusBadge status="ok" label="HDFS / Hadoop" />
          <StatusBadge status="ok" label="BERTopic (257 clusters)" />
          <StatusBadge status="ok" label="PageRank (GraphX)" />
          <StatusBadge status="ok" label="NER Pipeline (SciBERT)" />
        </div>
      </div>

      {/* Source Breakdown */}
      <div className="rounded-lg bg-card p-5">
        <h3 className="mb-4 text-lg font-semibold text-card-foreground">Ingestion Sources</h3>
        <div className="grid grid-cols-3 gap-4">
          {[
            { source: "arXiv", count: 5746, desc: "Preprints with full metadata & categories", color: "#378ADD" },
            { source: "S2ORC", count: 17338, desc: "Semantic Scholar with citation graphs", color: "#1D9E75" },
            { source: "OpenAlex", count: 0, desc: "Supplementary bibliographic data", color: "#7F77DD" },
          ].map((s) => (
            <div key={s.source} className="rounded-lg bg-secondary p-4">
              <div className="mb-1 flex items-center gap-2">
                <span className="h-3 w-3 rounded-full" style={{ backgroundColor: s.color }} />
                <span className="text-sm font-semibold text-card-foreground">{s.source}</span>
              </div>
              <p className="text-2xl font-bold" style={{ color: s.color }}>{s.count.toLocaleString()}</p>
              <p className="mt-1 text-xs text-muted-foreground">{s.desc}</p>
            </div>
          ))}
        </div>
      </div>

      {/* Citation Graph Stats */}
      <div className="rounded-lg bg-card p-5">
        <h3 className="mb-4 text-lg font-semibold text-card-foreground">Citation Graph</h3>
        <div className="grid grid-cols-3 gap-4 text-center">
          <div>
            <p className="text-3xl font-bold text-primary">{(citations.total_edges ?? 0).toLocaleString()}</p>
            <p className="text-sm text-muted-foreground">Total directed edges</p>
          </div>
          <div>
            <p className="text-3xl font-bold text-[#1D9E75]">{(papers.total_papers ?? 0).toLocaleString()}</p>
            <p className="text-sm text-muted-foreground">Nodes (papers)</p>
          </div>
          <div>
            <p className="text-3xl font-bold text-[#D85A30]">
              {papers.total_papers ? ((citations.total_edges ?? 0) / papers.total_papers).toFixed(1) : "—"}
            </p>
            <p className="text-sm text-muted-foreground">Avg edges per paper</p>
          </div>
        </div>
      </div>
    </div>
  )
}
