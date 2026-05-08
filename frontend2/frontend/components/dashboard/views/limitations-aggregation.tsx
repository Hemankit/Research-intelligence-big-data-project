"use client"

import { useEffect, useMemo, useState } from "react"
import { AlertTriangle } from "lucide-react"
import { fetchLimitationsAggregate } from "@/lib/api"

const categories = ["all", "cs.LG", "cs.CL", "cs.CV", "cs.AI", "cs.IR"]

const themeLabels: Record<string, string> = {
  data_scarcity: "Data Scarcity",
  compute_cost: "Compute Cost",
  generalization: "Generalization Gaps",
  evaluation_scope: "Evaluation Scope",
  interpretability: "Interpretability",
}

export function LimitationsAggregationView() {
  const [category, setCategory] = useState("all")
  const [loading, setLoading] = useState(true)
  const [result, setResult] = useState<any>(null)
  const [error, setError] = useState<string | null>(null)

  const loadData = async () => {
    setLoading(true)
    setError(null)
    try {
      const data = await fetchLimitationsAggregate({ category, limit: 300 })
      setResult(data)
    } catch (e: any) {
      setError(e.message || "Failed to load limitations aggregation")
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => {
    loadData()
  }, [category])

  const maxTheme = useMemo(() => {
    const items = result?.themes ?? []
    return items.length > 0 ? Math.max(...items.map((t: any) => t.count || 0), 1) : 1
  }, [result])

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-foreground">Limitations Aggregation Panel</h1>
        <p className="text-sm text-muted-foreground">
          Cluster recurring weaknesses and caveats mentioned in recent papers.
        </p>
      </div>

      <div className="rounded-lg bg-card p-5">
        <div className="flex items-center gap-3">
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
            onClick={loadData}
            className="h-10 rounded-lg bg-primary px-4 text-sm font-medium text-primary-foreground"
          >
            Refresh
          </button>
        </div>

        {error && <p className="mt-3 text-sm text-red-400">{error}</p>}
      </div>

      <div className="grid grid-cols-3 gap-4">
        <div className="rounded-lg bg-card p-4">
          <p className="text-xs uppercase tracking-wide text-muted-foreground">Scanned Papers</p>
          <p className="mt-1 text-2xl font-bold text-card-foreground">{result?.scanned_papers ?? "-"}</p>
        </div>
        <div className="rounded-lg bg-card p-4">
          <p className="text-xs uppercase tracking-wide text-muted-foreground">With Limitation Signals</p>
          <p className="mt-1 text-2xl font-bold text-[#D85A30]">{result?.papers_with_limitations ?? "-"}</p>
        </div>
        <div className="rounded-lg bg-card p-4">
          <p className="text-xs uppercase tracking-wide text-muted-foreground">Coverage Ratio</p>
          <p className="mt-1 text-2xl font-bold text-primary">
            {result?.coverage_ratio != null ? `${Math.round(result.coverage_ratio * 100)}%` : "-"}
          </p>
        </div>
      </div>

      <div className="grid grid-cols-2 gap-4">
        <div className="rounded-lg bg-card p-5">
          <h3 className="mb-4 text-base font-semibold text-card-foreground">Recurring Themes</h3>
          {loading ? (
            <p className="text-sm text-muted-foreground">Loading aggregated themes...</p>
          ) : result?.themes?.length > 0 ? (
            <ul className="space-y-3">
              {result.themes.map((theme: any) => (
                <li key={theme.theme} className="space-y-1">
                  <div className="flex items-center justify-between">
                    <span className="text-sm font-medium text-card-foreground">
                      {themeLabels[theme.theme] ?? theme.theme}
                    </span>
                    <span className="text-xs text-muted-foreground">{theme.count} papers</span>
                  </div>
                  <div className="h-1.5 w-full rounded-full bg-secondary">
                    <div
                      className="h-full rounded-full bg-[#D85A30]"
                      style={{ width: `${((theme.count || 0) / maxTheme) * 100}%` }}
                    />
                  </div>
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-sm text-muted-foreground">No limitation themes detected for this filter.</p>
          )}
        </div>

        <div className="rounded-lg bg-card p-5">
          <h3 className="mb-4 text-base font-semibold text-card-foreground">Representative Snippets</h3>
          {result?.themes?.length > 0 ? (
            <div className="space-y-3">
              {result.themes.slice(0, 3).map((theme: any) => (
                <div key={`${theme.theme}-examples`} className="rounded border border-border bg-secondary/20 p-3">
                  <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
                    {themeLabels[theme.theme] ?? theme.theme}
                  </p>
                  {theme.examples?.length > 0 ? (
                    <ul className="space-y-2">
                      {theme.examples.map((ex: any) => (
                        <li key={`${ex.paper_id}-${theme.theme}`}>
                          <p className="text-xs font-medium text-card-foreground">{ex.title}</p>
                          <p className="mt-1 text-xs text-muted-foreground">
                            <AlertTriangle className="mr-1 inline h-3 w-3" />
                            {ex.snippet || "Limitation cue found in abstract."}
                          </p>
                        </li>
                      ))}
                    </ul>
                  ) : (
                    <p className="text-xs text-muted-foreground">No examples captured.</p>
                  )}
                </div>
              ))}
            </div>
          ) : (
            <p className="text-sm text-muted-foreground">No representative snippets available.</p>
          )}
        </div>
      </div>
    </div>
  )
}
