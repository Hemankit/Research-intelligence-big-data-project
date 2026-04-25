"use client"

import { useEffect, useState } from "react"
import { fetchPapers } from "@/lib/api"

const SOURCE_COLORS: Record<string, string> = {
  arxiv:   "bg-primary/20 text-primary",
  s2orc:   "bg-[#1D9E75]/20 text-[#1D9E75]",
  openalex:"bg-[#7F77DD]/20 text-[#7F77DD]",
}

const CATEGORY_COLORS: Record<string, string> = {
  "cs.LG": "bg-primary/20 text-primary",
  "cs.CL": "bg-[#7F77DD]/20 text-[#7F77DD]",
  "cs.CV": "bg-[#1D9E75]/20 text-[#1D9E75]",
  "cs.AI": "bg-[#D85A30]/20 text-[#D85A30]",
  "cs.IR": "bg-[#BA7517]/20 text-[#BA7517]",
}

const SOURCES = ["all", "arxiv", "s2orc", "openalex"]

export function IngestionLogView() {
  const [papers, setPapers] = useState<any[]>([])
  const [loading, setLoading] = useState(true)
  const [activeSource, setActiveSource] = useState("all")
  const [page, setPage] = useState(0)
  const [totalCount, setTotalCount] = useState<number | null>(null)
  const PAGE_SIZE = 25

  useEffect(() => {
    setLoading(true)
    fetchPapers({
      source:   activeSource === "all" ? undefined : activeSource,
      limit:    PAGE_SIZE,
      offset:   page * PAGE_SIZE,
      sort_by:  "submitted_date",
    })
      .then((data) => {
        setPapers(data.papers ?? [])
        if (data.count !== undefined) setTotalCount(data.count)
      })
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [activeSource, page])

  // Group papers by month
  const grouped = papers.reduce((acc: Record<string, any[]>, paper) => {
    const month = paper.submitted_date?.slice(0, 7) ?? "Unknown"
    if (!acc[month]) acc[month] = []
    acc[month].push(paper)
    return acc
  }, {})

  const months = Object.keys(grouped).sort((a, b) => b.localeCompare(a))

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-foreground">Ingestion Log</h1>
        <p className="text-sm text-muted-foreground">
          Papers ingested into the corpus, grouped by submission date
        </p>
      </div>

      {/* Source filter + stats */}
      <div className="flex items-center justify-between">
        <div className="flex gap-2">
          {SOURCES.map((s) => (
            <button
              key={s}
              onClick={() => { setActiveSource(s); setPage(0) }}
              className={`rounded-full px-4 py-1.5 text-sm font-medium transition-colors capitalize ${
                activeSource === s
                  ? "bg-primary text-primary-foreground"
                  : "bg-secondary text-muted-foreground hover:bg-secondary/80 hover:text-foreground"
              }`}
            >
              {s}
            </button>
          ))}
        </div>
        <p className="text-sm text-muted-foreground">
          {totalCount != null ? `${totalCount.toLocaleString()} total papers` : ""}
        </p>
      </div>

      {/* Summary cards */}
      <div className="grid grid-cols-3 gap-4">
        {[
          { label: "arXiv Papers",  value: 5746,  color: "#378ADD" },
          { label: "S2ORC Papers",  value: 17338, color: "#1D9E75" },
          { label: "Total Ingested",value: 23084, color: "#7F77DD" },
        ].map((s) => (
          <div key={s.label} className="rounded-lg bg-card p-4">
            <p className="text-sm text-muted-foreground">{s.label}</p>
            <p className="text-2xl font-bold mt-1" style={{ color: s.color }}>
              {s.value.toLocaleString()}
            </p>
          </div>
        ))}
      </div>

      {/* Paper log grouped by month */}
      {loading ? (
        <div className="flex h-[300px] items-center justify-center rounded-lg bg-card">
          <p className="text-sm text-muted-foreground">Loading ingestion log...</p>
        </div>
      ) : (
        <div className="space-y-6">
          {months.map((month) => (
            <div key={month}>
              <div className="mb-3 flex items-center gap-3">
                <h3 className="text-sm font-semibold text-foreground">{month}</h3>
                <span className="rounded bg-secondary px-2 py-0.5 text-xs text-muted-foreground">
                  {grouped[month].length} papers
                </span>
                <div className="h-px flex-1 bg-border" />
              </div>
              <div className="rounded-lg bg-card overflow-hidden">
                <table className="w-full">
                  <tbody>
                    {grouped[month].map((paper: any, i: number) => (
                      <tr
                        key={paper.paper_id ?? i}
                        className="border-b border-border last:border-0 hover:bg-secondary/30 transition-colors"
                      >
                        <td className="px-4 py-3 text-xs text-muted-foreground font-mono w-28">
                          {paper.paper_id}
                        </td>
                        <td className="px-4 py-3 text-sm text-card-foreground">
                          {paper.title}
                        </td>
                        <td className="px-4 py-3 w-24">
                          {paper.primary_category && (
                            <span className={`rounded px-2 py-0.5 text-xs font-medium ${
                              CATEGORY_COLORS[paper.primary_category] ?? "bg-secondary text-muted-foreground"
                            }`}>
                              {paper.primary_category}
                            </span>
                          )}
                        </td>
                        <td className="px-4 py-3 w-20">
                          <span className={`rounded px-2 py-0.5 text-xs font-medium ${
                            SOURCE_COLORS[paper.source] ?? "bg-secondary text-muted-foreground"
                          }`}>
                            {paper.source}
                          </span>
                        </td>
                        <td className="px-4 py-3 text-right text-xs text-muted-foreground w-24">
                          {paper.submitted_date?.slice(0, 10)}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Pagination */}
      <div className="flex items-center justify-between">
        <button
          onClick={() => setPage(p => Math.max(0, p - 1))}
          disabled={page === 0}
          className="rounded-lg bg-secondary px-4 py-2 text-sm text-muted-foreground hover:bg-secondary/80 disabled:opacity-40"
        >
          ← Previous
        </button>
        <span className="text-sm text-muted-foreground">
          Page {page + 1} · Showing {page * PAGE_SIZE + 1}–{page * PAGE_SIZE + papers.length}
          {totalCount != null ? ` of ${totalCount.toLocaleString()}` : ""}
        </span>
        <button
          onClick={() => setPage(p => p + 1)}
          disabled={papers.length < PAGE_SIZE}
          className="rounded-lg bg-secondary px-4 py-2 text-sm text-muted-foreground hover:bg-secondary/80 disabled:opacity-40"
        >
          Next →
        </button>
      </div>
    </div>
  )
}
