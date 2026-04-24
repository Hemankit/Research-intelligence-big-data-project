/**
 * api/client.js
 * Thin axios wrapper that talks to your FastAPI backend.
 * All endpoints match api/main.py routes.
 */
import axios from 'axios'

const BASE = import.meta.env.VITE_API_URL ?? 'http://localhost:8000'

const http = axios.create({
  baseURL: BASE,
  timeout: 30_000,
  headers: { 'Content-Type': 'application/json' },
})

// ── Trend Explorer ──────────────────────────────────────────────
export const getTrends = (params) =>
  http.get('/api/trends', { params }).then(r => r.data)
// params: { domain, from_year, to_year, granularity }

export const getMethodAdoption = (params) =>
  http.get('/api/methods/adoption', { params }).then(r => r.data)
// params: { methods[], from_date, to_date }

// ── Paper Search / Influential Papers ─────────────────────────
export const searchPapers = (params) =>
  http.get('/api/papers/search', { params }).then(r => r.data)
// params: { q, domain, from_year, to_year, sort, page, size }

export const getInfluentialPapers = (params) =>
  http.get('/api/papers/influential', { params }).then(r => r.data)
// params: { domain, limit, min_pagerank }

export const getPaperById = (paperId) =>
  http.get(`/api/papers/${paperId}`).then(r => r.data)

// ── Topic / Landscape ──────────────────────────────────────────
export const getTopicClusters = (params) =>
  http.get('/api/topics/clusters', { params }).then(r => r.data)
// Returns BERTopic cluster list with UMAP coords

export const getLandscapePoints = (params) =>
  http.get('/api/topics/landscape', { params }).then(r => r.data)
// Returns {paper_id, x, y, cluster_id, title, pagerank}[]

// ── Entities ──────────────────────────────────────────────────
export const getTrendingEntities = (params) =>
  http.get('/api/entities/trending', { params }).then(r => r.data)
// params: { type: 'method'|'dataset'|'task', limit }

export const getEntityTimeline = (entity, type) =>
  http.get('/api/entities/timeline', { params: { entity, type } }).then(r => r.data)

// ── Citation Graph ─────────────────────────────────────────────
export const getCitationGraph = (params) =>
  http.get('/api/graph/citation', { params }).then(r => r.data)
// params: { paper_id?, domain, limit }

// ── Natural Language Query ─────────────────────────────────────
export const queryNL = (query, filters) =>
  http.post('/api/query', { query, filters }).then(r => r.data)

// ── Pipeline Status ────────────────────────────────────────────
export const getPipelineStatus = () =>
  http.get('/api/pipeline/status').then(r => r.data)

export const getCorpusStats = () =>
  http.get('/api/stats').then(r => r.data)
