"use client"

import { useState } from "react"
import { Search, BookOpen, Star, Zap, Shuffle } from "lucide-react"

const BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000"

const EXAMPLE_QUERIES = [
  "transformer attention mechanism",
  "graph neural networks node classification",
  "diffusion models image generation",
  "federated learning privacy",
]

const CATEGORY_CONFIG: Record<string, { label: string; color: string; icon: any; description: string }> = {
  foundational: {
    label: "Foundational Works",
    color: "text-[#BA7517] border-[#BA7517]/30 bg-[#BA7517]/10",
    icon: Star,
    description: "High-influence papers that shaped the field",
  },
  representative: {
    label: "Representative Methods",
    color: "text-primary border-primary/30 bg-primary/10",
    icon: BookOpen,
    description: "Central papers within major clusters",
  },
  emerging: {
    label: "Emerging Approaches",
    color: "text-[#1D9E75] border-[#1D9E75]/30 bg-[#1D9E75]/10",
    icon: Zap,
    description: "Recent, rapidly growing research directions",
  },
  contrasting: {
    label: "Diverse Perspectives",
    color: "text-[#7F77DD] border-[#7F77DD]/30 bg-[#7F77DD]/10",
    icon: Shuffle,
    description: "Different approaches to similar problems",
  },
}

interface PaperCardProps {
  paper: any
  categoryColor: string
}

function PaperCard({ paper, categoryColor }: PaperCardProps) {
  const [expanded, setExpanded] = useState(false)
  const hasInsights = paper.methodological_details?.length > 0 ||
    paper.limitations?.length > 0 ||
    paper.contributions?.length > 0

  return (
    <div className={`rounded-lg border p-4 ${categoryColor}`}>
      <div className="flex items-start justify-between gap-3">
        <div className="flex-1">
          <p className="text-sm font-semibold text-card-foreground leading-tight">{paper.title}</p>
          <div className="mt-1 flex flex-wrap items-center gap-2">
            {paper.primary_category && (
              <span className="rounded bg-secondary px-1.5 py-0.5 text-xs text-muted-foreground">
                {paper.primary_category}
              </span>
            )}
            {paper.submitted_date && (
              <span className="text-xs text-muted-foreground">{paper.submitted_date.slice(0, 7)}</span>
            )}
            {paper.pagerank_score != null && (
              <span className="text-xs text-primary font-medium">PR: {paper.pagerank_score.toFixed(2)}</span>
            )}
          </div>
          {paper.selection_reason && (
            <p className="mt-1.5 text-xs text-muted-foreground italic">{paper.selection_reason}</p>
          )}
        </div>
        <div className="flex flex-col items-end gap-1 shrink-0">
          {paper.composite_score != null && (
            <span className="text-xs font-bold text-card-foreground">
              {(paper.composite_score * 100).toFixed(0)}%
            </span>
          )}
          {paper.signal_scores && (
            <div className="flex gap-1">
              {Object.entries(paper.signal_scores).slice(0, 3).map(([key, val]: [string, any]) => (
                <div key={key} title={key} className="h-1.5 w-6 rounded-full bg-secondary overflow-hidden">
                  <div className="h-full rounded-full bg-primary" style={{ width: `${val * 100}%` }} />
                </div>
              ))}
            </div>
          )}
        </div>
      </div>

      {hasInsights && (
        <button
          onClick={() => setExpanded(!expanded)}
          className="mt-2 text-xs text-muted-foreground hover:text-foreground"
        >
          {expanded ? "Hide insights ▲" : "Show full-text insights ▼"}
        </button>
      )}

      {expanded && (
        <div className="mt-3 space-y-2 border-t border-border pt-3">
          {paper.contributions?.length > 0 && (
            <div>
              <p className="text-xs font-medium text-card-foreground mb-1">Contributions</p>
              {paper.contributions.slice(0, 2).map((c: string, i: number) => (
                <p key={i} className="text-xs text-muted-foreground">• {c}</p>
              ))}
            </div>
          )}
          {paper.limitations?.length > 0 && (
            <div>
              <p className="text-xs font-medium text-card-foreground mb-1">Limitations</p>
              {paper.limitations.slice(0, 2).map((l: string, i: number) => (
                <p key={i} className="text-xs text-muted-foreground">• {l}</p>
              ))}
            </div>
          )}
          {paper.methodological_details?.length > 0 && (
            <div>
              <p className="text-xs font-medium text-card-foreground mb-1">Methods</p>
              {paper.methodological_details.slice(0, 2).map((m: string, i: number) => (
                <p key={i} className="text-xs text-muted-foreground">• {m}</p>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

export function ReadingPathView() {
  const [query, setQuery] = useState("")
  const [result, setResult] = useState<any>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [pipelineWarning, setPipelineWarning] = useState(false)

  const handleSearch = async (q: string) => {
    if (!q.trim()) return
    setLoading(true)
    setError(null)
    setPipelineWarning(false)
    setResult(null)

    try {
      const res = await fetch(`${BASE}/api/analyze?q=${encodeURIComponent(q)}`)
      if (res.status === 503) {
        setPipelineWarning(true)
        setError("Analysis pipeline is loading SciBERT model. Please wait 30 seconds and try again.")
        return
      }
      if (!res.ok) throw new Error(`API error ${res.status}`)
      const data = await res.json()
      setResult(data)
    } catch (e: any) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  const readingGuide = result?.reading_guide ?? {}
  const totalPapers = result?.selected_count ?? 0

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-foreground">Curated Reading Path</h1>
        <p className="text-sm text-muted-foreground">
          Enter a research topic to get a curated set of high-value papers organized by role
        </p>
      </div>

      {/* Search */}
      <div className="space-y-2">
        <div className="relative flex gap-2">
          <div className="relative flex-1">
            <Search className="absolute left-4 top-1/2 h-5 w-5 -translate-y-1/2 text-muted-foreground" />
            <input
              type="text"
              placeholder='e.g. "transformer attention mechanism" or "graph neural networks"'
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={(e) => e.key === "Enter" && handleSearch(query)}
              className="h-12 w-full rounded-lg bg-card pl-12 pr-4 text-sm text-card-foreground placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-primary"
            />
          </div>
          <button
            onClick={() => handleSearch(query)}
            disabled={loading || !query.trim()}
            className="h-12 rounded-lg bg-primary px-6 text-sm font-medium text-primary-foreground disabled:opacity-50"
          >
            {loading ? "Analyzing..." : "Analyze"}
          </button>
        </div>
        <div className="flex flex-wrap gap-2">
          {EXAMPLE_QUERIES.map((q) => (
            <button
              key={q}
              onClick={() => { setQuery(q); handleSearch(q) }}
              className="rounded-full bg-secondary px-3 py-1 text-xs text-muted-foreground hover:bg-secondary/80 hover:text-foreground"
            >
              {q}
            </button>
          ))}
        </div>
      </div>

      {/* How it works */}
      <div className="rounded-lg bg-card p-5">
        <h3 className="mb-3 text-base font-semibold text-card-foreground">How It Works</h3>
        <div className="grid grid-cols-2 gap-6">
          <div>
            <p className="text-sm text-muted-foreground leading-relaxed">
              Enter a research topic and the system selects 10–50 high-value papers from the corpus
              using a multi-signal ranking engine, then organizes them into a structured reading path
              so you can efficiently navigate the literature.
            </p>
            <p className="text-sm text-muted-foreground leading-relaxed mt-2">
              Papers with full text available are analyzed deeper with SciBERT to extract
              contributions, limitations, and methodological details.
            </p>
          </div>
          <div className="space-y-2">
            <p className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Selection Signals</p>
            {[
              { label: "Relevance", desc: "Elasticsearch match score against your query" },
              { label: "Influence", desc: "PageRank score from the citation graph" },
              { label: "Recency", desc: "Publication date where newer papers score higher" },
              { label: "Representativeness", desc: "Centrality within its BERTopic cluster" },
              { label: "Diversity", desc: "Coverage across different research directions" },
            ].map((s) => (
              <div key={s.label} className="flex items-start gap-2">
                <span className="mt-0.5 h-2 w-2 shrink-0 rounded-full bg-primary" />
                <p className="text-xs text-muted-foreground">
                  <span className="font-medium text-card-foreground">{s.label}</span> — {s.desc}
                </p>
              </div>
            ))}
          </div>
        </div>
      </div>

      {/* Pipeline warning */}
      {pipelineWarning && (
        <div className="rounded-lg border border-[#BA7517]/30 bg-[#BA7517]/10 p-4">
          <p className="text-sm text-[#BA7517]">{error}</p>
          <p className="text-xs text-muted-foreground mt-1">
            The SciBERT model loads on first use. After it warms up, subsequent queries will be fast.
          </p>
        </div>
      )}

      {/* Error */}
      {error && !pipelineWarning && (
        <p className="text-sm text-red-400">{error}</p>
      )}

      {/* Loading */}
      {loading && (
        <div className="rounded-lg bg-card p-8 text-center">
          <p className="text-sm text-muted-foreground">Running selective analysis pipeline...</p>
          <p className="text-xs text-muted-foreground mt-1">Selecting and analyzing high-value papers with SciBERT</p>
        </div>
      )}

      

      {/* Results */}
      {result && !loading && (
        <div className="space-y-6">
          {/* Summary */}
          <div className="rounded-lg bg-card p-4 flex items-center justify-between">
            <div>
              <p className="text-sm font-semibold text-card-foreground">
                Query: <span className="text-primary">"{result.query}"</span>
              </p>
              <p className="text-xs text-muted-foreground mt-0.5">
                {totalPapers} papers selected · {result.duration_seconds?.toFixed(1)}s
                {result.cached && " · cached"}
              </p>
            </div>
          </div>

          {/* Categories */}
          {Object.entries(CATEGORY_CONFIG).map(([key, config]) => {
            const papers = readingGuide[key] ?? []
            if (papers.length === 0) return null
            const Icon = config.icon
            return (
              <div key={key}>
                <div className="flex items-center gap-2 mb-3">
                  <Icon className="h-4 w-4 text-muted-foreground" />
                  <h3 className="text-base font-semibold text-card-foreground">{config.label}</h3>
                  <span className="rounded bg-secondary px-2 py-0.5 text-xs text-muted-foreground">
                    {papers.length} papers
                  </span>
                  <p className="text-xs text-muted-foreground">{config.description}</p>
                </div>
                <div className="grid grid-cols-2 gap-3">
                  {papers.map((paper: any) => (
                    <PaperCard key={paper.paper_id} paper={paper} categoryColor={config.color} />
                  ))}
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
