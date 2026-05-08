const BASE = process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000"

type TrendsResponse = { count: number; trends: any[] }
type PapersResponse = { count: number; papers: any[] }
type StatsResponse = Record<string, any>
type LandscapeResponse = { count: number; points: any[]; note?: string | null }
type IdeaValidationResponse = {
  query: string
  count: number
  summary: {
    supporting_signals: number
    cautionary_signals: number
    support_ratio: number
  }
  topic_distribution: any[]
  evidence: any[]
}
type LimitationsAggregateResponse = {
  scanned_papers: number
  papers_with_limitations: number
  coverage_ratio: number
  themes: any[]
}

async function getJson<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`)
  if (!res.ok) {
    throw new Error(`API error ${res.status} for ${path}`)
  }
  return res.json()
}

export async function fetchStats(): Promise<StatsResponse> {
  return getJson<StatsResponse>("/api/stats")
}

export async function fetchTopPapers(limit = 10, category?: string): Promise<PapersResponse> {
  const params = new URLSearchParams({ limit: String(limit) })
  if (category) params.set("category", category)
  return getJson<PapersResponse>(`/api/papers/influential?${params.toString()}`)
}

export async function fetchTrends(category?: string): Promise<TrendsResponse> {
  const params = new URLSearchParams()
  if (category) params.set("category", category)
  const query = params.toString()
  return getJson<TrendsResponse>(`/api/trends${query ? `?${query}` : ""}`)
}

export async function fetchPapers(params?: {
  category?: string
  source?: string
  limit?: number
  offset?: number
  sort_by?: string
  order?: "ASC" | "DESC"
}): Promise<PapersResponse> {
  const qp = new URLSearchParams()
  if (params?.category) qp.set("category", params.category)
  if (params?.source) qp.set("source", params.source)
  if (params?.limit != null) qp.set("limit", String(params.limit))
  if (params?.offset != null) qp.set("offset", String(params.offset))
  if (params?.sort_by) qp.set("sort_by", params.sort_by)
  if (params?.order) qp.set("order", params.order)
  const query = qp.toString()
  return getJson<PapersResponse>(`/api/papers/list${query ? `?${query}` : ""}`)
}

export async function searchPapers(q: string, size = 20): Promise<PapersResponse> {
  const params = new URLSearchParams({ q, size: String(size) })
  return getJson<PapersResponse>(`/api/papers/search?${params.toString()}`)
}

export async function fetchPaperDetail(paperId: string): Promise<any> {
  return getJson<any>(`/api/papers/${encodeURIComponent(paperId)}`)
}

export async function fetchCitations(paperId: string): Promise<any> {
  return getJson<any>(`/api/graph/citation/${encodeURIComponent(paperId)}?direction=both&limit=50`)
}

export async function fetchLandscape(params?: {
  limit?: number
  category?: string
  start?: string
  end?: string
  min_pagerank?: number
}): Promise<LandscapeResponse> {
  const qp = new URLSearchParams()
  qp.set("limit", String(params?.limit ?? 3000))
  if (params?.category && params.category !== "all") qp.set("category", params.category)
  if (params?.start) qp.set("start", params.start)
  if (params?.end) qp.set("end", params.end)
  if (params?.min_pagerank != null) qp.set("min_pagerank", String(params.min_pagerank))
  return getJson<LandscapeResponse>(`/api/topics/landscape?${qp.toString()}`)
}

export async function validateIdea(params: {
  q: string
  category?: string
  limit?: number
}): Promise<IdeaValidationResponse> {
  const qp = new URLSearchParams({ q: params.q })
  if (params.category && params.category !== "all") qp.set("category", params.category)
  if (params.limit != null) qp.set("limit", String(params.limit))
  return getJson<IdeaValidationResponse>(`/api/ideas/validate?${qp.toString()}`)
}

export async function fetchLimitationsAggregate(params?: {
  category?: string
  topic_cluster?: string
  limit?: number
}): Promise<LimitationsAggregateResponse> {
  const qp = new URLSearchParams()
  if (params?.category && params.category !== "all") qp.set("category", params.category)
  if (params?.topic_cluster) qp.set("topic_cluster", params.topic_cluster)
  if (params?.limit != null) qp.set("limit", String(params.limit))
  const query = qp.toString()
  return getJson<LimitationsAggregateResponse>(`/api/limitations/aggregate${query ? `?${query}` : ""}`)
}
