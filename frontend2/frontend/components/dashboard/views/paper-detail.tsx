"use client"

import { useEffect, useState } from "react"
import { ArrowLeft } from "lucide-react"
import { fetchPaperDetail, fetchCitations } from "@/lib/api"

interface PaperDetailProps {
  paperId: string
  onBack: () => void
}

const categoryColors: Record<string, string> = {
  "cs.CL": "bg-primary/20 text-primary",
  "cs.CV": "bg-[#7F77DD]/20 text-[#7F77DD]",
  "cs.LG": "bg-[#1D9E75]/20 text-[#1D9E75]",
  "cs.AI": "bg-[#D85A30]/20 text-[#D85A30]",
  "cs.IR": "bg-[#BA7517]/20 text-[#BA7517]",
}

export function PaperDetailView({ paperId, onBack }: PaperDetailProps) {
  const [paper, setPaper] = useState<any>(null)
  const [citations, setCitations] = useState<any>(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  useEffect(() => {
    setLoading(true)
    setError(null)
    Promise.all([fetchPaperDetail(paperId), fetchCitations(paperId)])
      .then(([paperData, citationData]) => {
        setPaper(paperData)
        setCitations(citationData)
      })
      .catch((err) => setError(err.message))
      .finally(() => setLoading(false))
  }, [paperId])

  if (loading) {
    return (
      <div className="flex h-[400px] items-center justify-center rounded-lg bg-card">
        <p className="text-muted-foreground">Loading paper...</p>
      </div>
    )
  }

  if (error || !paper) {
    return (
      <div className="space-y-4">
        <button onClick={onBack} className="flex items-center gap-2 text-sm text-muted-foreground hover:text-foreground">
          <ArrowLeft className="h-4 w-4" /> Back
        </button>
        <div className="flex h-[300px] items-center justify-center rounded-lg bg-card">
          <p className="text-sm text-muted-foreground">{error ?? "Paper not found."}</p>
        </div>
      </div>
    )
  }

  const pageRank = paper.pagerank_score ?? 0
  const circumference = 2 * Math.PI * 45
  const strokeDashoffset = circumference - pageRank * circumference

  const parseArray = (val: any): string[] => {
  if (!val) return []
  if (Array.isArray(val)) return val
  if (typeof val === 'string') {
    try { return JSON.parse(val) } catch { return [] }
  }
  return []
  }
const methods = parseArray(paper.methods)
const tasks = parseArray(paper.tasks)
const datasets = parseArray(paper.datasets)

  const citedBy: any[] = citations?.cited_by ?? []
  const references: any[] = citations?.references ?? []

  // Build citation graph SVG nodes
  const citingNodes = citedBy.slice(0, 5).map((_, i) => {
    const angle = (Math.PI * 2 * i) / 5 - Math.PI / 2
    return { x: 150 + Math.cos(angle) * 90, y: 100 + Math.sin(angle) * 70, type: "citing" }
  })
  const refNodes = references.slice(0, 3).map((_, i) => {
    const angle = (Math.PI * 2 * i) / 3 + Math.PI / 2
    return { x: 150 + Math.cos(angle) * 80, y: 100 + Math.sin(angle) * 60, type: "ref" }
  })
  const allNodes = [...citingNodes, ...refNodes]

  return (
    <div className="space-y-6">
      <button
        onClick={onBack}
        className="flex items-center gap-2 text-sm text-muted-foreground transition-colors hover:text-foreground"
      >
        <ArrowLeft className="h-4 w-4" />
        Back to Knowledge Table
      </button>

      <p className="text-sm text-muted-foreground">{paper.paper_id} — Full metadata and citation graph</p>

      <div className="grid grid-cols-3 gap-6">
        <div className="col-span-2 space-y-4">
          <h1 className="text-2xl font-bold text-foreground">{paper.title}</h1>
          <p className="text-sm text-muted-foreground">
            {Array.isArray(paper.authors) ? paper.authors.join(", ") : paper.authors}
          </p>
          <div className="flex flex-wrap items-center gap-2">
            {paper.primary_category && (
              <span className={`rounded px-2 py-0.5 text-xs font-medium ${categoryColors[paper.primary_category] ?? "bg-secondary text-muted-foreground"}`}>
                {paper.primary_category}
              </span>
            )}
            {paper.topic_cluster && paper.topic_cluster !== "outlier" && (
              <span className="rounded bg-[#7F77DD]/20 px-2 py-0.5 text-xs font-medium text-[#7F77DD]">
                {paper.topic_cluster}
              </span>
            )}
            {paper.submitted_date && (
              <span className="text-xs text-muted-foreground">{paper.submitted_date.slice(0, 10)}</span>
            )}
          </div>
          <p className="text-sm leading-relaxed text-muted-foreground">{paper.abstract}</p>
        </div>

        <div className="flex flex-col items-center justify-center rounded-lg bg-card p-6">
          <div className="relative">
            <svg className="h-28 w-28 -rotate-90">
              <circle cx="56" cy="56" r="45" fill="none" stroke="#2D3139" strokeWidth="8" />
              <circle
                cx="56"
                cy="56"
                r="45"
                fill="none"
                stroke="#378ADD"
                strokeWidth="8"
                strokeDasharray={circumference}
                strokeDashoffset={strokeDashoffset}
                strokeLinecap="round"
              />
            </svg>
            <div className="absolute inset-0 flex items-center justify-center">
              <span className="text-2xl font-bold text-primary">
                {pageRank > 0 ? pageRank.toFixed(2) : "—"}
              </span>
            </div>
          </div>
          <p className="mt-3 text-sm text-muted-foreground">PageRank score</p>
          {paper.citation_count != null && (
            <p className="mt-1 text-lg font-semibold text-card-foreground">
              {paper.citation_count.toLocaleString()} citations
            </p>
          )}
          {paper.reference_count != null && (
            <p className="text-xs text-muted-foreground">{paper.reference_count} references</p>
          )}
        </div>
      </div>

      {(methods.length > 0 || tasks.length > 0 || datasets.length > 0) && (
        <div className="rounded-lg bg-card p-5">
          <h3 className="mb-4 text-lg font-semibold text-card-foreground">NER Entities</h3>
          <div className="space-y-3">
            {methods.length > 0 && (
              <div>
                <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">Methods</p>
                <div className="flex flex-wrap gap-2">
                  {methods.map((m) => (
                    <span key={m} className="rounded-full bg-primary/20 px-3 py-1 text-xs font-medium text-primary">
                      {m}
                    </span>
                  ))}
                </div>
              </div>
            )}
            {tasks.length > 0 && (
              <div>
                <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">Tasks</p>
                <div className="flex flex-wrap gap-2">
                  {tasks.map((t) => (
                    <span key={t} className="rounded-full bg-[#1D9E75]/20 px-3 py-1 text-xs font-medium text-[#1D9E75]">
                      {t}
                    </span>
                  ))}
                </div>
              </div>
            )}
            {datasets.length > 0 && (
              <div>
                <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">Datasets</p>
                <div className="flex flex-wrap gap-2">
                  {datasets.map((d) => (
                    <span key={d} className="rounded-full bg-[#BA7517]/20 px-3 py-1 text-xs font-medium text-[#BA7517]">
                      {d}
                    </span>
                  ))}
                </div>
              </div>
            )}
          </div>
        </div>
      )}

      <div className="grid grid-cols-2 gap-4">
        <div className="rounded-lg bg-card p-5">
          <h3 className="mb-4 text-lg font-semibold text-card-foreground">Citation Graph</h3>
          {allNodes.length > 0 ? (
            <>
              <svg viewBox="0 0 300 200" className="h-[200px] w-full">
                {allNodes.map((node, i) => (
                  <line
                    key={i}
                    x1="150" y1="100"
                    x2={node.x} y2={node.y}
                    stroke={node.type === "citing" ? "#378ADD" : "#4B5563"}
                    strokeWidth="1"
                  />
                ))}
                {allNodes.map((node, i) => (
                  <circle
                    key={i}
                    cx={node.x} cy={node.y} r="10"
                    fill={node.type === "citing" ? "#378ADD" : "#4B5563"}
                  />
                ))}
                <circle cx="150" cy="100" r="20" fill="#1E3A5F" stroke="#378ADD" strokeWidth="2" />
              </svg>
              <div className="mt-4 flex items-center justify-center gap-4 text-xs text-muted-foreground">
                <div className="flex items-center gap-1.5">
                  <span className="h-3 w-3 rounded-full bg-primary" /> Citing ({citedBy.length})
                </div>
                <div className="flex items-center gap-1.5">
                  <span className="h-3 w-3 rounded-full bg-[#1E3A5F] ring-1 ring-primary" /> This paper
                </div>
                <div className="flex items-center gap-1.5">
                  <span className="h-3 w-3 rounded-full bg-[#4B5563]" /> References ({references.length})
                </div>
              </div>
            </>
          ) : (
            <p className="py-8 text-center text-sm text-muted-foreground">No citation edges found for this paper.</p>
          )}
        </div>

        <div className="rounded-lg bg-card p-5">
          <h3 className="mb-4 text-lg font-semibold text-card-foreground">
            {paper.topic_cluster && paper.topic_cluster !== "outlier"
              ? `Related Papers (${paper.topic_cluster})`
              : "Citing Papers"}
          </h3>
          {citedBy.length > 0 ? (
            <ul className="space-y-4">
              {citedBy.slice(0, 4).map((c: any, i: number) => (
                <li key={c.paper_id ?? i} className="space-y-1">
                  <p className="text-sm font-medium text-card-foreground">{c.title || c.paper_id}</p>
                  {c.pagerank_score != null && (
                    <p className="text-xs text-muted-foreground">
                      PageRank: <span className="text-primary">{c.pagerank_score.toFixed(2)}</span>
                    </p>
                  )}
                </li>
              ))}
            </ul>
          ) : (
            <p className="text-sm text-muted-foreground">No citing papers found in corpus.</p>
          )}
        </div>
      </div>
    </div>
  )
}
