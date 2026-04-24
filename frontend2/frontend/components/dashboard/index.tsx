"use client"

import { useState } from "react"
import { Sidebar } from "./sidebar"
import { OverviewView } from "./views/overview"
import { TrendExplorerView } from "./views/trend-explorer"
import { TopicLandscapeView } from "./views/topic-landscape"
import { KnowledgeTableView } from "./views/knowledge-table"
import { PaperDetailView } from "./views/paper-detail"
import { GraphStatsView } from "./views/graph-stats"

export function Dashboard() {
  const [activeView, setActiveView] = useState("overview")
  const [selectedPaperId, setSelectedPaperId] = useState<string | null>(null)

  const handlePaperClick = (paperId: string) => {
    setSelectedPaperId(paperId)
  }

  const handleBackFromPaper = () => {
    setSelectedPaperId(null)
  }

  const renderView = () => {
    if (selectedPaperId) {
      return <PaperDetailView paperId={selectedPaperId} onBack={handleBackFromPaper} />
    }

    switch (activeView) {
      case "overview":
        return <OverviewView />
      case "trends":
        return <TrendExplorerView />
      case "topics":
        return <TopicLandscapeView />
      case "table":
        return <KnowledgeTableView onPaperClick={handlePaperClick} />
      case "graph":
        return <GraphStatsView />
      default:
        return (
          <div className="flex h-[400px] items-center justify-center rounded-lg bg-card">
            <p className="text-muted-foreground">View coming soon...</p>
          </div>
        )
    }
  }

  return (
    <div className="flex min-h-screen">
      <Sidebar activeView={activeView} onViewChange={(view) => { setActiveView(view); setSelectedPaperId(null); }} />
      <main className="ml-[200px] flex-1 overflow-auto p-6">
        <div className="mx-auto max-w-6xl">
          {renderView()}
        </div>
      </main>
    </div>
  )
}
