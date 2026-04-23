/**
 * Global state store (Zustand)
 * Manages filters, active query, and UI state.
 */
import { create } from 'zustand'

export const useStore = create((set, get) => ({
  // ── Active filters ─────────────────────────────────────────
  filters: {
    domains:    ['cs.LG', 'cs.CL'],
    fromYear:   2021,
    toYear:     2025,
    minPagerank: 20,
    paperTypes: ['Preprint', 'Conference'],
  },
  setFilter: (key, value) =>
    set(s => ({ filters: { ...s.filters, [key]: value } })),

  // ── Query ──────────────────────────────────────────────────
  query: '',
  setQuery: (q) => set({ query: q }),

  // ── Active tab ─────────────────────────────────────────────
  activeTab: 'trends',
  setActiveTab: (tab) => set({ activeTab: tab }),

  // ── Selected paper ─────────────────────────────────────────
  selectedPaper: null,
  setSelectedPaper: (p) => set({ selectedPaper: p }),

  // ── Selected cluster (landscape) ──────────────────────────
  selectedCluster: null,
  setSelectedCluster: (c) => set({ selectedCluster: c }),

  // ── Sidebar open ──────────────────────────────────────────
  sidebarOpen: true,
  toggleSidebar: () => set(s => ({ sidebarOpen: !s.sidebarOpen })),
}))
