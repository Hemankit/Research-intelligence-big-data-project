"use client"

import { useEffect, useState, useCallback } from "react"
import { Search } from "lucide-react"
import { cn } from "@/lib/utils"
import { fetchPapers, searchPapers } from "@/lib/api"

interface KnowledgeTableProps {
  onPaperClick: (paperId: string) => void
}

const sources = ["arxiv", "s2orc", "openalex"]

const categoryColors: Record<string, string> = {
  "cs.CL": "bg-primary/20 text-primary",
  "cs.CV": "bg-[#7F77DD]/20 text-[#7F77DD]",
  "cs.LG": "bg-[#1D9E75]/20 text-[#1D9E75]",
  "cs.AI": "bg-[#D85A30]/20 text-[#D85A30]",
  "cs.IR": "bg-[#BA7517]/20 text-[#BA7517]",
}

export function KnowledgeTableView({ onPaperClick }: KnowledgeTableProps) {
  const [papers, setPapers] = useState<any[]>([])
  const [searchQuery, setSearchQuery] = useState("")
  const [activeSource, setActiveSource] = useState<string | null>(null)
  const [loading, setLoading] = useState(true)
  const [totalCount, setTotalCount] = useState<number | null>(null)

  const loadPapers = useCallback(() => {
    setLoading(true)
    const request =
      searchQuery.length > 2
        ? searchPapers(searchQuery)
        : fetchPapers({ source: activeSource || undefined, limit: 20, sort_by: "citation_count" })

    request
      .then((data) => {
        setPapers(data.papers || [])
        if (data.count !== undefined) setTotalCount(data.count)
      })
      .catch(console.error)
      .finally(() => setLoading(false))
  }, [searchQuery, activeSource])

  useEffect(() => {
    const debounce = setTimeout(loadPapers, 300)
    return () => clearTimeout(debounce)
  }, [loadPapers])

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-4">
        <div className="relative flex-1">
          <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <input
            type="text"
            placeholder="Search titles and abstracts..."
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            className="h-10 w-full rounded-lg bg-card pl-10 pr-4 text-sm text-card-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary"
          />
        </div>
        <div className="flex gap-2">
          {sources.map((source) => (
            <button
              key={source}
              onClick={() => setActiveSource(activeSource === source ? null : source)}
              className={cn(
                "rounded-full px-3 py-1.5 text-xs font-medium transition-colors",
                activeSource === source
                  ? "bg-primary text-primary-foreground"
                  : "bg-secondary text-muted-foreground hover:bg-secondary/80"
              )}
            >
              {source}
            </button>
          ))}
        </div>
      </div>

      <div className="rounded-lg bg-card">
        {loading ? (
          <div className="flex h-[300px] items-center justify-center">
            <p className="text-sm text-muted-foreground">Loading papers...</p>
          </div>
        ) : papers.length === 0 ? (
          <div className="flex h-[300px] items-center justify-center">
            <p className="text-sm text-muted-foreground">No papers found.</p>
          </div>
        ) : (
          <table className="w-full">
            <thead>
              <tr className="border-b border-border">
                <th className="px-5 py-3 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                  Title
                </th>
                <th className="px-5 py-3 text-left text-xs font-medium uppercase tracking-wider text-muted-foreground">
                  Category
                </th>
                <th className="px-5 py-3 text-right text-xs font-medium uppercase tracking-wider text-muted-foreground">
                  Citations
                </th>
                <th className="px-5 py-3 text-right text-xs font-medium uppercase tracking-wider text-muted-foreground">
                  PageRank
                </th>
              </tr>
            </thead>
            <tbody>
              {papers.map((paper: any) => (
                <tr
                  key={paper.paper_id}
                  onClick={() => onPaperClick(paper.paper_id)}
                  className="cursor-pointer border-b border-border transition-colors last:border-0 hover:bg-secondary/50"
                >
                  <td className="px-5 py-4 text-sm font-medium text-card-foreground">{paper.title}</td>
                  <td className="px-5 py-4">
                    <span
                      className={cn(
                        "rounded px-2 py-0.5 text-xs font-medium",
                        categoryColors[paper.primary_category] ?? "bg-secondary text-muted-foreground"
                      )}
                    >
                      {paper.primary_category}
                    </span>
                  </td>
                  <td className="px-5 py-4 text-right text-sm text-muted-foreground">
                    {paper.citation_count != null ? paper.citation_count.toLocaleString() : "—"}
                  </td>
                  <td className="px-5 py-4 text-right text-sm font-bold text-primary">
                    {paper.pagerank_score != null ? paper.pagerank_score.toFixed(2) : "—"}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>

      <p className="text-sm text-muted-foreground">
        Showing {papers.length} {totalCount != null ? `of ${totalCount.toLocaleString()}` : ""} results
      </p>
    </div>
  )
}
