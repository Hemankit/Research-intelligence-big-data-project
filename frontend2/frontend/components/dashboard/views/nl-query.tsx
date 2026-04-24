"use client"

import { useState } from "react"
import { Search, X } from "lucide-react"

const EXAMPLE_QUERIES = [
  "Emerging trends in Graph Neural Networks",
  "Datasets used in diffusion model research",
  "Research clusters in the LLM space",
  "Recent papers on quantum computing",
]

interface QueryResult {
  papers: any[]
  summary: string
  total: number
}

export function NLQueryBar() {
  const [query, setQuery] = useState("")
  const [result, setResult] = useState<QueryResult | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [isOpen, setIsOpen] = useState(false)

  const handleSearch = async (q: string) => {
    if (!q.trim()) return
    setLoading(true)
    setError(null)
    setIsOpen(true)

    try {
      // Use Elasticsearch search via /api/papers/search
      const res = await fetch(
        `${process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000"}/api/papers/search?q=${encodeURIComponent(q)}&size=5`
      )
      if (!res.ok) throw new Error(`Search failed: ${res.status}`)
      const data = await res.json()
      const papers = data.papers ?? []
      setResult({
        papers,
        total: data.count ?? papers.length,
        summary: `Found ${data.count ?? papers.length} papers matching "${q}". Showing top ${papers.length} by relevance.`,
      })
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  const handleClose = () => {
    setIsOpen(false)
    setResult(null)
    setError(null)
    setQuery("")
  }

  return (
    <div className="mb-6">
      {/* Search bar */}
      <div className="relative flex items-center gap-2">
        <div className="relative flex-1">
          <Search className="absolute left-4 top-1/2 h-5 w-5 -translate-y-1/2 text-muted-foreground" />
          <input
            type="text"
            placeholder='Try: "Emerging trends in Graph Neural Networks over 6 months"'
            value={query}
            onChange={(e) => setQuery(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && handleSearch(query)}
            className="h-12 w-full rounded-lg bg-card pl-12 pr-4 text-sm text-card-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary"
          />
        </div>
        <button
          onClick={() => handleSearch(query)}
          disabled={loading || !query.trim()}
          className="h-12 rounded-lg bg-primary px-6 text-sm font-medium text-primary-foreground transition-colors hover:bg-primary/90 disabled:opacity-50"
        >
          {loading ? "Searching..." : "Explore"}
        </button>
      </div>

      {/* Example queries */}
      <div className="mt-2 flex flex-wrap gap-2">
        {EXAMPLE_QUERIES.map((q) => (
          <button
            key={q}
            onClick={() => { setQuery(q); handleSearch(q) }}
            className="rounded-full bg-secondary px-3 py-1 text-xs text-muted-foreground transition-colors hover:bg-secondary/80 hover:text-foreground"
          >
            {q}
          </button>
        ))}
      </div>

      {/* Results panel */}
      {isOpen && (
        <div className="mt-4 rounded-lg border border-border bg-card p-5">
          <div className="mb-4 flex items-center justify-between">
            <h3 className="text-lg font-semibold text-card-foreground">
              {loading ? "Searching..." : "Search Results"}
            </h3>
            <button onClick={handleClose} className="text-muted-foreground hover:text-foreground">
              <X className="h-5 w-5" />
            </button>
          </div>

          {error && (
            <p className="text-sm text-red-400">{error}</p>
          )}

          {!loading && result && (
            <>
              <p className="mb-4 text-sm text-muted-foreground">{result.summary}</p>
              <div className="space-y-3">
                {result.papers.map((paper: any, i: number) => (
                  <div key={paper.paper_id ?? i} className="rounded-lg bg-secondary p-4">
                    <div className="flex items-start justify-between gap-4">
                      <div className="flex-1">
                        <p className="text-sm font-semibold text-card-foreground">{paper.title}</p>
                        <p className="mt-1 text-xs text-muted-foreground">
                          {Array.isArray(paper.authors)
                            ? paper.authors.slice(0, 3).join(", ") + (paper.authors.length > 3 ? " et al." : "")
                            : paper.authors}
                          {paper.submitted_date && ` · ${paper.submitted_date.slice(0, 7)}`}
                        </p>
                        {paper.abstract && (
                          <p className="mt-2 text-xs text-muted-foreground line-clamp-2">{paper.abstract}</p>
                        )}
                      </div>
                      <div className="flex flex-col items-end gap-1 shrink-0">
                        {paper.pagerank_score != null && (
                          <span className="text-sm font-bold text-primary">
                            {paper.pagerank_score.toFixed(2)}
                          </span>
                        )}
                        {paper.primary_category && (
                          <span className="rounded bg-primary/20 px-2 py-0.5 text-xs text-primary">
                            {paper.primary_category}
                          </span>
                        )}
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            </>
          )}
        </div>
      )}
    </div>
  )
}
