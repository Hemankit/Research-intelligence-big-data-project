"use client"

import { LayoutDashboard, TrendingUp, ScatterChart, Table, FileText, Database, Network, Cpu, BookOpen } from "lucide-react"
import { cn } from "@/lib/utils"

interface SidebarProps {
  activeView: string
  onViewChange: (view: string) => void
}

const mainNavItems = [
  { id: "overview", label: "Overview", icon: LayoutDashboard },
  { id: "trends", label: "Trend Explorer", icon: TrendingUp },
  { id: "topics", label: "Topic Landscape", icon: ScatterChart },
  { id: "table", label: "Knowledge Table", icon: Table },
]

const dataNavItems = [
  { id: "ingestion", label: "Ingestion Log", icon: Database },
  { id: "graph", label: "Graph Stats", icon: Network },
  { id: "ner", label: "NER Pipeline", icon: Cpu },
]

export function Sidebar({ activeView, onViewChange }: SidebarProps) {
  return (
    <aside className="fixed left-0 top-0 z-40 flex h-screen w-[200px] flex-col bg-sidebar">
      <div className="flex items-center gap-2 px-4 py-5">
        <BookOpen className="h-6 w-6 text-primary" />
        <span className="text-lg font-semibold text-sidebar-foreground">ResearchIQ</span>
      </div>

      <nav className="flex-1 px-2 py-4">
        <div className="mb-2 px-3 text-xs font-medium uppercase tracking-wider text-muted-foreground">
          Main
        </div>
        <ul className="space-y-1">
          {mainNavItems.map((item) => (
            <li key={item.id}>
              <button
                onClick={() => onViewChange(item.id)}
                className={cn(
                  "flex w-full items-center gap-3 rounded-r-md px-3 py-2 text-sm font-medium transition-colors",
                  activeView === item.id
                    ? "border-l-2 border-primary bg-sidebar-accent text-sidebar-accent-foreground"
                    : "text-muted-foreground hover:bg-sidebar-accent/50 hover:text-sidebar-foreground"
                )}
              >
                <item.icon className="h-4 w-4" />
                {item.label}
              </button>
            </li>
          ))}
        </ul>

        <div className="mb-2 mt-6 px-3 text-xs font-medium uppercase tracking-wider text-muted-foreground">
          Data
        </div>
        <ul className="space-y-1">
          {dataNavItems.map((item) => (
            <li key={item.id}>
              <button
                onClick={() => onViewChange(item.id)}
                className={cn(
                  "flex w-full items-center gap-3 rounded-r-md px-3 py-2 text-sm font-medium transition-colors",
                  activeView === item.id
                    ? "border-l-2 border-primary bg-sidebar-accent text-sidebar-accent-foreground"
                    : "text-muted-foreground hover:bg-sidebar-accent/50 hover:text-sidebar-foreground"
                )}
              >
                <item.icon className="h-4 w-4" />
                {item.label}
              </button>
            </li>
          ))}
        </ul>
      </nav>

      <div className="border-t border-sidebar-border px-4 py-4">
        <p className="text-xs text-muted-foreground">Last ingested</p>
        <p className="text-sm text-sidebar-foreground">2 hours ago</p>
      </div>
    </aside>
  )
}
