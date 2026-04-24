import {
  LineChart, Line, BarChart, Bar, XAxis, YAxis,
  CartesianGrid, Tooltip, ResponsiveContainer, Legend,
} from 'recharts'
import { useTrends, useMethodAdoption } from '../hooks/useData'
import { useStore } from '../hooks/useStore'

const CUSTOM_TOOLTIP_STYLE = {
  background: 'var(--bg3)',
  border: '1px solid var(--border2)',
  borderRadius: 6,
  padding: '8px 12px',
  fontSize: 11,
  fontFamily: 'var(--font-mono)',
  color: 'var(--ink)',
}

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div style={CUSTOM_TOOLTIP_STYLE}>
      <div style={{ color: 'var(--ink3)', marginBottom: 6, fontSize: 10 }}>{label}</div>
      {payload.map(p => (
        <div key={p.name} style={{ display: 'flex', gap: 8, alignItems: 'center', marginBottom: 2 }}>
          <span style={{ width: 8, height: 8, borderRadius: '50%', background: p.color, display: 'inline-block' }} />
          <span style={{ color: 'var(--ink2)' }}>{p.name}</span>
          <span style={{ marginLeft: 'auto', fontWeight: 500 }}>{p.value}</span>
        </div>
      ))}
    </div>
  )
}

function MethodTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div style={CUSTOM_TOOLTIP_STYLE}>
      <div style={{ color: 'var(--ink3)', marginBottom: 6, fontSize: 10 }}>{label}</div>
      {payload.map(p => (
        <div key={p.dataKey} style={{ display: 'flex', gap: 8, marginBottom: 2 }}>
          <span style={{ color: p.fill === '#3b82f6' ? 'var(--ink3)' : 'var(--blue)' }}>
            {p.dataKey === 'count_2023' ? '2023' : '2024'}
          </span>
          <span style={{ marginLeft: 'auto', fontWeight: 500 }}>{p.value?.toLocaleString()}</span>
        </div>
      ))}
    </div>
  )
}

export default function TrendExplorer() {
  const { filters } = useStore()
  const { data: trends, loading: tLoading } = useTrends(filters)
  const { data: methods, loading: mLoading } = useMethodAdoption(filters)

  // Build recharts-friendly data
  const trendData = trends
    ? trends.labels.map((label, i) => {
        const obj = { label }
        trends.series.forEach(s => { obj[s.name] = s.data[i] })
        return obj
      })
    : []

  return (
    <div style={{ display: 'flex', flexDirection: 'column', gap: 16 }}>
      {/* Trend Line Chart */}
      <ChartCard
        title="Topic Frequency Over Time"
        subtitle="Publications per quarter, normalized by corpus size"
        loading={tLoading}
      >
        {trends && (
          <>
            <div style={{ display: 'flex', gap: 16, marginBottom: 12, flexWrap: 'wrap' }}>
              {trends.series.map(s => (
                <div key={s.name} style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 10, color: 'var(--ink3)' }}>
                  <span style={{ width: 20, height: 2, background: s.color, display: 'inline-block', borderRadius: 1 }} />
                  {s.name}
                </div>
              ))}
            </div>
            <ResponsiveContainer width="100%" height={220}>
              <LineChart data={trendData} margin={{ top: 4, right: 8, bottom: 0, left: -20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" />
                <XAxis
                  dataKey="label" tick={{ fill: 'var(--ink3)', fontSize: 9, fontFamily: 'var(--font-mono)' }}
                  axisLine={false} tickLine={false}
                />
                <YAxis tick={{ fill: 'var(--ink3)', fontSize: 9, fontFamily: 'var(--font-mono)' }} axisLine={false} tickLine={false} />
                <Tooltip content={<ChartTooltip />} />
                {trends.series.map((s, i) => (
                  <Line
                    key={s.name}
                    type="monotone"
                    dataKey={s.name}
                    stroke={s.color}
                    strokeWidth={i === 0 ? 2 : 1.5}
                    dot={false}
                    activeDot={{ r: 3, fill: s.color }}
                    strokeDasharray={i > 1 ? '4 3' : undefined}
                  />
                ))}
              </LineChart>
            </ResponsiveContainer>
          </>
        )}
      </ChartCard>

      {/* Method Adoption Bar Chart */}
      <ChartCard
        title="Method Adoption Curve"
        subtitle="Unique papers citing each method — 2023 vs 2024"
        loading={mLoading}
      >
        {methods && (
          <>
            <div style={{ display: 'flex', gap: 16, marginBottom: 12 }}>
              <div style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 10, color: 'var(--ink3)' }}>
                <span style={{ width: 10, height: 10, borderRadius: 2, background: 'rgba(59,130,246,0.25)', display: 'inline-block' }} />
                2023
              </div>
              <div style={{ display: 'flex', alignItems: 'center', gap: 5, fontSize: 10, color: 'var(--ink3)' }}>
                <span style={{ width: 10, height: 10, borderRadius: 2, background: '#3b82f6', display: 'inline-block' }} />
                2024
              </div>
            </div>
            <ResponsiveContainer width="100%" height={180}>
              <BarChart data={methods} margin={{ top: 4, right: 8, bottom: 0, left: -20 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="rgba(255,255,255,0.04)" vertical={false} />
                <XAxis
                  dataKey="method" tick={{ fill: 'var(--ink3)', fontSize: 9, fontFamily: 'var(--font-mono)' }}
                  axisLine={false} tickLine={false}
                />
                <YAxis
                  tick={{ fill: 'var(--ink3)', fontSize: 9, fontFamily: 'var(--font-mono)' }}
                  axisLine={false} tickLine={false}
                  tickFormatter={v => v >= 1000 ? `${(v/1000).toFixed(0)}K` : v}
                />
                <Tooltip content={<MethodTooltip />} />
                <Bar dataKey="count_2023" fill="rgba(59,130,246,0.2)" radius={[2,2,0,0]} />
                <Bar dataKey="count_2024" fill="#3b82f6" radius={[2,2,0,0]} />
              </BarChart>
            </ResponsiveContainer>
          </>
        )}
      </ChartCard>
    </div>
  )
}

function ChartCard({ title, subtitle, loading, children }) {
  return (
    <div style={{
      background: 'var(--bg2)',
      border: '1px solid var(--border)',
      borderRadius: 'var(--radius-lg)',
      padding: '16px 18px',
    }}>
      <div style={{ marginBottom: 12 }}>
        <div style={{ fontFamily: 'var(--font-display)', fontSize: 15, color: 'var(--ink)' }}>{title}</div>
        <div style={{ fontSize: 10, color: 'var(--ink3)', marginTop: 2 }}>{subtitle}</div>
      </div>
      {loading
        ? <div style={{ height: 200, display: 'flex', alignItems: 'center', justifyContent: 'center', color: 'var(--ink3)', fontSize: 11 }}>Loading...</div>
        : children
      }
    </div>
  )
}
