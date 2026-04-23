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

export function useStats() {
  return useFetch(
    () => USE_MOCK ? Promise.resolve(mock.MOCK_STATS) : api.getCorpusStats(),
    []
  )
}

export function useTrends(filters) {
  return useFetch(
    () => USE_MOCK
      ? Promise.resolve(mock.MOCK_TRENDS)
      : api.getTrends(filters),
    [JSON.stringify(filters)]
  )
}

export function useMethodAdoption(filters) {
  return useFetch(
    () => USE_MOCK
      ? Promise.resolve(mock.MOCK_METHOD_ADOPTION)
      : api.getMethodAdoption(filters),
    [JSON.stringify(filters)]
  )
}

export function useInfluentialPapers(filters) {
  return useFetch(
    () => USE_MOCK
      ? Promise.resolve(mock.MOCK_INFLUENTIAL_PAPERS)
      : api.getInfluentialPapers(filters),
    [JSON.stringify(filters)]
  )
}

export function useTrendingEntities() {
  return useFetch(
    () => USE_MOCK
      ? Promise.resolve(mock.MOCK_ENTITIES)
      : Promise.all([
          api.getTrendingEntities({ type: 'method',  limit: 14 }),
          api.getTrendingEntities({ type: 'dataset', limit: 12 }),
          api.getTrendingEntities({ type: 'task',    limit: 8  }),
        ]).then(([methods, datasets, tasks]) => ({ methods, datasets, tasks })),
    []
  )
}

export function useLandscape(filters) {
  return useFetch(
    () => USE_MOCK
      ? Promise.resolve(mock.MOCK_LANDSCAPE)
      : api.getLandscapePoints(filters),
    [JSON.stringify(filters)]
  )
}

export function usePipelineStatus() {
  return useFetch(
    () => USE_MOCK
      ? Promise.resolve(mock.MOCK_PIPELINE_STATUS)
      : api.getPipelineStatus(),
    []
  )
}

export function useTrendingTopics() {
  return useFetch(
    () => USE_MOCK
      ? Promise.resolve(mock.MOCK_TRENDING)
      : api.getTrendingEntities({ type: 'topic', limit: 7 }),
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
