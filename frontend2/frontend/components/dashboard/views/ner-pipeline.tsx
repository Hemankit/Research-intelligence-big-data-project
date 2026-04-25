"use client"

import { useEffect, useState } from "react"

const BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000"

async function fetchEntities(type: string, limit: number) {
  const res = await fetch(`${BASE}/api/entities/trending/es?type=${type}&limit=${limit}`)
  if (!res.ok) return []
  const data = await res.json()
  return data.entities ?? []
}

const COLORS = {
  method:  ["#378ADD", "bg-primary/20 text-primary"],
  dataset: ["#BA7517", "bg-[#BA7517]/20 text-[#BA7517]"],
  task:    ["#1D9E75", "bg-[#1D9E75]/20 text-[#1D9E75]"],
}

interface EntityListProps {
  title: string
  entities: any[]
  colorClass: string
  barColor: string
  loading: boolean
}

function EntityList({ title, entities, colorClass, barColor, loading }: EntityListProps) {
  const max = entities[0]?.paper_count ?? 1
  return (
    <div className="rounded-lg bg-card p-5">
      <h3 className="mb-4 text-lg font-semibold text-card-foreground">{title}</h3>
      {loading ? (
        <p className="text-sm text-muted-foreground">Loading...</p>
      ) : entities.length === 0 ? (
        <p className="text-sm text-muted-foreground">No data available.</p>
      ) : (
        <ul className="space-y-3">
          {entities.map((e: any) => (
            <li key={e.entity} className="space-y-1">
              <div className="flex items-center justify-between">
                <span className={`rounded-full px-2 py-0.5 text-xs font-medium ${colorClass}`}>
                  {e.entity}
                </span>
                <span className="text-xs text-muted-foreground">{e.paper_count} papers</span>
              </div>
              <div className="h-1.5 w-full rounded-full bg-secondary">
                <div
                  className="h-full rounded-full transition-all"
                  style={{ width: `${(e.paper_count / max) * 100}%`, backgroundColor: barColor }}
                />
              </div>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

export function NERPipelineView() {
  const [methods,  setMethods]  = useState<any[]>([])
  const [datasets, setDatasets] = useState<any[]>([])
  const [tasks,    setTasks]    = useState<any[]>([])
  const [loading,  setLoading]  = useState(true)

  useEffect(() => {
    setLoading(true)
    Promise.all([
      fetchEntities("method",  15),
      fetchEntities("dataset", 12),
      fetchEntities("task",    12),
    ]).then(([m, d, t]) => {
      setMethods(m)
      setDatasets(d)
      setTasks(t)
    }).catch(console.error)
      .finally(() => setLoading(false))
  }, [])

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold text-foreground">NER Pipeline</h1>
        <p className="text-sm text-muted-foreground">
          Named entities extracted from paper abstracts using SciBERT — methods, datasets, and tasks
        </p>
      </div>

      {/* Pipeline info cards */}
      <div className="grid grid-cols-3 gap-4">
        {[
          { label: "Papers Processed", value: "5,602", sub: "arXiv papers with NER data", color: "#378ADD" },
          { label: "NER Model",        value: "SciBERT", sub: "allenai/scibert_scivocab_uncased", color: "#7F77DD" },
          { label: "Entity Types",     value: "3",     sub: "Methods, Datasets, Tasks", color: "#1D9E75" },
        ].map((s) => (
          <div key={s.label} className="rounded-lg bg-card p-4">
            <p className="text-sm text-muted-foreground">{s.label}</p>
            <p className="text-2xl font-bold mt-1" style={{ color: s.color }}>{s.value}</p>
            <p className="text-xs text-muted-foreground mt-1">{s.sub}</p>
          </div>
        ))}
      </div>

      {/* Pipeline architecture */}
      <div className="rounded-lg bg-card p-5">
        <h3 className="mb-4 text-lg font-semibold text-card-foreground">Pipeline Architecture</h3>
        <div className="flex items-center gap-2 flex-wrap">
          {[
            "Abstract Text",
            "→",
            "Text Cleaning (spaCy)",
            "→",
            "Tokenization",
            "→",
            "SciBERT NER",
            "→",
            "Entity Extraction",
            "→",
            "HDFS Storage",
            "→",
            "Elasticsearch Index",
          ].map((step, i) => (
            step === "→" ? (
              <span key={i} className="text-muted-foreground">→</span>
            ) : (
              <span key={i} className="rounded bg-secondary px-3 py-1.5 text-xs font-medium text-card-foreground">
                {step}
              </span>
            )
          ))}
        </div>
      </div>

      {/* Entity lists */}
      <div className="grid grid-cols-3 gap-4">
        <EntityList
          title="Top Methods"
          entities={methods}
          colorClass={COLORS.method[1]}
          barColor={COLORS.method[0]}
          loading={loading}
        />
        <EntityList
          title="Top Datasets"
          entities={datasets}
          colorClass={COLORS.dataset[1]}
          barColor={COLORS.dataset[0]}
          loading={loading}
        />
        <EntityList
          title="Top Tasks"
          entities={tasks}
          colorClass={COLORS.task[1]}
          barColor={COLORS.task[0]}
          loading={loading}
        />
      </div>
    </div>
  )
}
