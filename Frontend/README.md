# Research Intelligence — Frontend

React + Vite dashboard. Talks to your FastAPI backend (`/api/*`).

## Quick Start

```bash
cd frontend

# Install dependencies
npm install

# Copy env file and configure
cp .env.local.example .env.local

# Dev mode with mock data (no backend needed)
VITE_USE_MOCK=true npm run dev

# Dev mode against live backend
VITE_USE_MOCK=false VITE_API_URL=http://localhost:8000 npm run dev

# Production build
npm run build
```

## Structure

```
src/
├── api/
│   ├── client.js     # axios wrappers for all FastAPI endpoints
│   └── mock.js       # drop-in mock data for local dev
├── components/
│   ├── Topbar.jsx         # header + live badge
│   ├── QueryBar.jsx       # NL query input
│   ├── Sidebar.jsx        # domain/time/influence filters
│   ├── SnapshotCards.jsx  # 4 KPI cards
│   ├── TrendExplorer.jsx  # recharts line + bar charts
│   ├── LandscapeMap.jsx   # UMAP canvas scatter plot
│   ├── InfluentialPapers.jsx  # sortable paper list
│   └── RightPanel.jsx     # trending / entities / pipeline / graph
├── hooks/
│   ├── useStore.js   # Zustand global state (filters, active tab)
│   └── useData.js    # data-fetching hooks (mock-aware)
├── styles/
│   └── globals.css   # design tokens + base styles
├── App.jsx           # root layout
└── main.jsx          # entry point
```

## API Endpoints Consumed

| Hook                  | Endpoint                        |
|-----------------------|---------------------------------|
| `useStats`            | `GET /api/stats`                |
| `useTrends`           | `GET /api/trends`               |
| `useMethodAdoption`   | `GET /api/methods/adoption`     |
| `useInfluentialPapers`| `GET /api/papers/influential`   |
| `useLandscape`        | `GET /api/topics/landscape`     |
| `useTrendingEntities` | `GET /api/entities/trending`    |
| `usePipelineStatus`   | `GET /api/pipeline/status`      |
| `useNLQuery`          | `POST /api/query`               |

All hooks automatically fall back to mock data when `VITE_USE_MOCK=true`.

## Adding a New Chart

1. Add endpoint to `api/client.js`
2. Add mock data to `api/mock.js`
3. Add a fetch hook to `hooks/useData.js`
4. Create component in `components/`
5. Add to `App.jsx` or a tab
