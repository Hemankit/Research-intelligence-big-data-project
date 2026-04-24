/**
 * hooks/useData.js
 * Data-fetching hooks. Swap VITE_USE_MOCK=true to use local mock data.
 */
import { useState, useEffect, useCallback } from 'react'
import * as api from '../api/client'
import * as mock from '../api/mock'

const USE_MOCK = import.meta.env.VITE_USE_MOCK === 'true'

function useFetch(fetcher, deps = []) {
  const [data,    setData]    = useState(null)
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)

  const run = useCallback(async () => {
    setLoading(true)
    setError(null)
    try {
      const result = await fetcher()
      setData(result)
    } catch (e) {
      setError(e.message ?? 'Request failed')
    } finally {
      setLoading(false)
    }
  }, deps) // eslint-disable-line

  useEffect(() => { run() }, [run])
  return { data, loading, error, refetch: run }
}

// /api/stats → { papers:{total_papers,total_categories,earliest_paper,latest_paper},
//                citations:{total_edges}, pagerank:{...}, trends:{...} }
// Components expect flat: { total_papers, total_categories, total_edges, ... }
export function useStats() {
  return useFetch(
    () => USE_MOCK
      ? Promise.resolve(mock.MOCK_STATS)
      : api.getCorpusStats().then(d => ({
          total_papers:     d.papers?.total_papers     ?? 0,
          total_categories: d.papers?.total_categories ?? 0,
          earliest_paper:   d.papers?.earliest_paper   ?? null,
          latest_paper:     d.papers?.latest_paper     ?? null,
          total_edges:      d.citations?.total_edges   ?? 0,
          scored_papers:    d.pagerank?.scored_papers  ?? 0,
          months_covered:   d.trends?.months_covered   ?? 0,
          topic_count:      d.trends?.topic_count      ?? 0,
        })),
    []
  )
}

// /api/trends → { count, trends: [{primary_category, topic_cluster, year_month, paper_count, ...}] }
// Component expects: { labels: string[], series: [{name, data: number[]}] }
export function useTrends(filters) {
  return useFetch(
    () => USE_MOCK
      ? Promise.resolve(mock.MOCK_TRENDS)
      : api.getTrends(filters).then(d => {
          if (!d?.trends?.length) return { labels: [], series: [] }
          const labels = [...new Set(d.trends.map(r => r.year_month))].sort()
          const seriesMap = {}
          d.trends.forEach(r => {
            const key = r.primary_category || r.topic_cluster || 'unknown'
            if (!seriesMap[key]) seriesMap[key] = {}
            seriesMap[key][r.year_month] = r.paper_count
          })
          const series = Object.entries(seriesMap).map(([name, byMonth]) => ({
            name,
            data: labels.map(l => byMonth[l] ?? 0)
          }))
          return { labels, series }
        }),
    [JSON.stringify(filters)]
  )
}

// /api/methods/adoption → { count, data: [{topic_cluster, year_month, paper_count, avg_pagerank}] }
// Component expects: { labels, series } same shape as trends
export function useMethodAdoption(filters) {
  return useFetch(
    () => USE_MOCK
      ? Promise.resolve(mock.MOCK_METHOD_ADOPTION)
      : api.getMethodAdoption(filters).then(d => {
          if (!d?.data?.length) return { labels: [], series: [] }
          const labels = [...new Set(d.data.map(r => r.year_month))].sort()
          const seriesMap = {}
          d.data.forEach(r => {
            const key = r.topic_cluster || 'unknown'
            if (!seriesMap[key]) seriesMap[key] = {}
            seriesMap[key][r.year_month] = r.paper_count
          })
          const series = Object.entries(seriesMap).map(([name, byMonth]) => ({
            name,
            data: labels.map(l => byMonth[l] ?? 0)
          }))
          return { labels, series }
        }),
    [JSON.stringify(filters)]
  )
}

// /api/papers/influential → { count, papers: [{paper_id, title, authors, ...}] }
// Component expects array of paper objects directly
export function useInfluentialPapers(filters) {
  return useFetch(
    () => USE_MOCK
      ? Promise.resolve(mock.MOCK_INFLUENTIAL_PAPERS)
      : api.getInfluentialPapers(filters).then(d => d?.papers ?? []),
    [JSON.stringify(filters)]
  )
}

// /api/entities/trending → { count, type, entities: [{entity, paper_count}] }
// Component expects: { methods: [], datasets: [], tasks: [] }
export function useTrendingEntities() {
  return useFetch(
    () => USE_MOCK
      ? Promise.resolve(mock.MOCK_ENTITIES)
      : Promise.all([
          api.getTrendingEntities({ type: 'method',  limit: 14 }),
          api.getTrendingEntities({ type: 'dataset', limit: 12 }),
          api.getTrendingEntities({ type: 'task',    limit: 8  }),
        ]).then(([methods, datasets, tasks]) => ({
          methods:  methods.entities  ?? [],
          datasets: datasets.entities ?? [],
          tasks:    tasks.entities    ?? [],
        })),
    []
  )
}

// /api/topics/landscape → { count, points: [{paper_id, title, umap_x, umap_y, topic_cluster, ...}] }
// Component expects array of point objects directly
export function useLandscape(filters) {
  return useFetch(
    () => USE_MOCK
      ? Promise.resolve(mock.MOCK_LANDSCAPE)
      : api.getLandscapePoints(filters).then(d => d?.points ?? []),
    [JSON.stringify(filters)]
  )
}

// /api/pipeline/status → { status, components, counts }
// Component expects array of { label, status } objects
export function usePipelineStatus() {
  return useFetch(
    () => USE_MOCK
      ? Promise.resolve(mock.MOCK_PIPELINE_STATUS)
      : api.getPipelineStatus().then(d => [
          { label: 'Hive',              status: d.components?.hive === 'ok' ? 'done' : 'error' },
          { label: 'Elasticsearch',     status: d.components?.elasticsearch === 'ok' ? 'done' : 'error' },
          { label: 'Analysis Pipeline', status: d.components?.analysis_pipeline === 'ready' ? 'done' : 'queued' },
          { label: 'Papers Indexed',    status: 'done', count: d.counts?.papers },
          { label: 'Citation Edges',    status: 'done', count: d.counts?.citation_edges },
        ]),
    []
  )
}

// /api/entities/trending?type=topic → { count, type, entities: [{entity, paper_count}] }
// Component expects array of { name, pct, color, count }
export function useTrendingTopics() {
  return useFetch(
    () => USE_MOCK
      ? Promise.resolve(mock.MOCK_TRENDING)
      : api.getTrendingEntities({ type: 'topic', limit: 7 }).then(d => {
          const entities = d?.entities ?? []
          const max = entities[0]?.paper_count ?? 1
          const colors = ['#7c6af7','#f7826a','#6af7c2','#f7d06a','#6ab4f7','#f76adb','#a8f76a']
          return entities.map((e, i) => ({
            name:  e.entity,
            pct:   Math.round((e.paper_count / max) * 100),
            color: colors[i % colors.length],
            count: e.paper_count,
          }))
        }),
    []
  )
}

export function useNLQuery() {
  const [result,  setResult]  = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)

  const submit = useCallback(async (query, filters) => {
    setLoading(true)
    setError(null)
    try {
      const data = USE_MOCK
        ? await new Promise(r => setTimeout(() => r({
            clusters: ['Transformers', 'RLHF'],
            papers: mock.MOCK_INFLUENTIAL_PAPERS.slice(0, 4),
            summary: `Found 4 highly relevant papers for "${query}". Top cluster: Transformers (62%). Emerging trend: DPO alignment (+31% last 6mo).`,
          }), 800))
        : await api.queryNL(query, filters)
      setResult(data)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }, [])

  return { result, loading, error, submit }
}
