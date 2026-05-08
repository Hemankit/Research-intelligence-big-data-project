"use client"

import { useMemo, useState } from "react"
import { Lightbulb, Search } from "lucide-react"
import { validateIdea } from "@/lib/api"

const categories = ["all", "cs.LG", "cs.CL", "cs.CV", "cs.AI", "cs.IR"]

export function IdeaValidationView() {
  const [query, setQuery] = useState("")
  const [category, setCategory] = useState("all")
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)

  const supportPercent = useMemo(() => {
    const ratio = result?.summary?.support_ratio
    if (ratio == null) return 0
    return Math.round(ratio * 100)
  }, [result])

  const runValidation = async () => {
    if (query.trim().length < 3) return
    setLoading(true)
    setError(null)
    try {
      const data = await validateIdea({ q: query.trim(), category, limit: 24 })
      setResult(data)
    } catch (e: any) {
      setError(e.message || "Validation failed")
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-foreground">Idea Validation Panel</h1>
        <p className="text-sm text-muted-foreground">
          Quickly test whether your research idea has supporting or cautionary signals in the corpus.
        </p>
      </div>

      <div className="rounded-lg bg-card p-5 space-y-4">
        <div className="flex flex-wrap gap-3">
          <div className="relative min-w-[320px] flex-1">
            <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
            <input
              type="text"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && runValidation()}
              placeholder="e.g. diffusion model for low-resource OCR"
              className="h-10 w-full rounded-lg bg-secondary pl-10 pr-3 text-sm text-card-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary"
            />
          </div>

          <select
            value={category}
            onChange={(e) => setCategory(e.target.value)}
            className="h-10 rounded-lg bg-secondary px-3 text-sm text-card-foreground focus:outline-none focus:ring-2 focus:ring-primary"
          >
            {categories.map((c) => (
              <option key={c} value={c}>
                {c === "all" ? "All categories" : c}
              </option>
            ))}
          </select>

          <button
            onClick={runValidation}
            disabled={loading || query.trim().length < 3}
            className="h-10 rounded-lg bg-primary px-4 text-sm font-medium text-primary-foreground disabled:opacity-50"
          >
            {loading ? "Validating..." : "Validate"}
          </button>
        </div>

        {error && <p className="text-sm text-red-400">{error}</p>}
      </div>

      {result && (
        <>
          <div className="grid grid-cols-4 gap-4">
            <div className="rounded-lg bg-card p-4">
              <p className="text-xs uppercase tracking-wide text-muted-foreground">Evidence Papers</p>
              <p className="mt-1 text-2xl font-bold text-card-foreground">{result.count}</p>
            </div>
            <div className="rounded-lg bg-card p-4">
              <p className="text-xs uppercase tracking-wide text-muted-foreground">Supporting Signals</p>
              <p className="mt-1 text-2xl font-bold text-[#1D9E75]">{result.summary.supporting_signals}</p>
            </div>
            <div className="rounded-lg bg-card p-4">
              <p className="text-xs uppercase tracking-wide text-muted-foreground">Cautionary Signals</p>
              <p className="mt-1 text-2xl font-bold text-[#D85A30]">{result.summary.cautionary_signals}</p>
            </div>
            <div className="rounded-lg bg-card p-4">
              <p className="text-xs uppercase tracking-wide text-muted-foreground">Support Ratio</p>
              <p className="mt-1 text-2xl font-bold text-primary">{supportPercent}%</p>
            </div>
          </div>

          <div className="grid grid-cols-3 gap-4">
            <div className="rounded-lg bg-card p-5">
              <h3 className="mb-3 text-base font-semibold text-card-foreground">Top Topic Clusters</h3>
              {result.topic_distribution?.length > 0 ? (
                <ul className="space-y-3">
                  {result.topic_distribution.map((t: any, idx: number) => (
                    <li key={`${t.topic_cluster}-${idx}`} className="flex items-center justify-between text-sm">
                      <span className="truncate text-card-foreground">{t.topic_cluster || "Unknown"}</span>
                      <span className="text-muted-foreground">{t.paper_count}</span>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-sm text-muted-foreground">No topic distribution available.</p>
              )}
            </div>

            <div className="col-span-2 rounded-lg bg-card p-5">
              <h3 className="mb-3 text-base font-semibold text-card-foreground">Evidence Feed</h3>
              {result.evidence?.length > 0 ? (
                <ul className="space-y-3">
                  {result.evidence.map((paper: any) => (
                    <li key={paper.paper_id} className="rounded border border-border bg-secondary/20 p-3">
                      <div className="flex items-start justify-between gap-3">
                        <div>
                          <p className="text-sm font-medium text-card-foreground leading-tight">{paper.title}</p>
                          <p className="mt-1 text-xs text-muted-foreground">
                            {paper.primary_category || "Unknown"}
                            {paper.submitted_date ? ` · ${String(paper.submitted_date).slice(0, 7)}` : ""}
                            {paper.topic_cluster ? ` · ${paper.topic_cluster}` : ""}
                          </p>
                        </div>
                        <div className="inline-flex items-center gap-1 rounded-full bg-primary/15 px-2 py-0.5 text-xs font-medium text-primary">
                          <Lightbulb className="h-3 w-3" />
                          PR {paper.pagerank_score != null ? Number(paper.pagerank_score).toFixed(2) : "-"}
                        </div>
                      </div>
                      {paper.limitation_snippets?.length > 0 && (
                        <p className="mt-2 text-xs text-muted-foreground">Potential caveat: {paper.limitation_snippets[0]}</p>
                      )}
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-sm text-muted-foreground">No evidence papers found for this idea.</p>
              )}
            </div>
          </div>
        </>
      )}
    </div>
  )
}
