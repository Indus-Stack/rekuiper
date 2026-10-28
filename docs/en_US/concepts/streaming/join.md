# Joining Sources

In rekuiper, `JOIN` operations correlate data from multiple sources.

rekuiper supports two join combinations:

- **Stream-to-Stream Join**: Joins multiple unbounded streams. Stream-to-stream joins require an explicit window definition.
- **Stream-to-Table Join**: Joins a stream to a table. Arriving stream events trigger join calculations against current table records.

Supported join types include `INNER`, `LEFT`, `RIGHT`, `FULL`, `CROSS`, and temporal `ASOF JOIN` / `LEFT ASOF JOIN` (which matches each fast stream event with the most recent reference event at or before its timestamp without requiring a window cross-product).

<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 860 205" width="100%" height="auto" role="img" aria-label="Streaming Join Patterns in rekuiper" font-family="-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif">
  <rect x="0" y="0" width="860" height="205" rx="10" fill="#f8fafc" stroke="#e2e8f0" stroke-width="1" />
  <text x="430" y="28" text-anchor="middle" font-size="14" font-weight="700" fill="#0f172a">Streaming Join Topologies in rekuiper</text>
  <rect x="25" y="48" width="250" height="135" rx="8" fill="#ffffff" stroke="#cbd5e1" stroke-width="1.5" />
  <text x="150" y="72" text-anchor="middle" font-size="12" font-weight="700" fill="#1e40af">1. Stream-to-Stream Join</text>
  <text x="150" y="94" text-anchor="middle" font-size="11" fill="#334155">Stream A + Stream B in Window</text>
  <text x="150" y="114" text-anchor="middle" font-size="10" fill="#64748b">INNER / LEFT / RIGHT / FULL / CROSS</text>
  <text x="150" y="134" text-anchor="middle" font-size="10" fill="#64748b">Requires GROUP BY Window Clause</text>
  <text x="150" y="154" text-anchor="middle" font-size="10" fill="#2563eb">SharedMap Copy-on-Write Buffers</text>
  <rect x="305" y="48" width="250" height="135" rx="8" fill="#ffffff" stroke="#cbd5e1" stroke-width="1.5" />
  <text x="430" y="72" text-anchor="middle" font-size="12" font-weight="700" fill="#c2410c">2. Stream-to-Table Join</text>
  <text x="430" y="94" text-anchor="middle" font-size="11" fill="#334155">Live Stream + Lookup / Scan Table</text>
  <text x="430" y="114" text-anchor="middle" font-size="10" fill="#64748b">Enriches Events on Arrival</text>
  <text x="430" y="134" text-anchor="middle" font-size="10" fill="#64748b">Backed by Memory, Redis, or SQL</text>
  <text x="430" y="154" text-anchor="middle" font-size="10" fill="#ea580c">O(1)VecDeque Bounded Eviction</text>
  <rect x="585" y="48" width="250" height="135" rx="8" fill="#ffffff" stroke="#10b981" stroke-width="1.5" />
  <text x="710" y="72" text-anchor="middle" font-size="12" font-weight="700" fill="#047857">3. Temporal ASOF JOIN</text>
  <text x="710" y="94" text-anchor="middle" font-size="11" fill="#334155">ASOF JOIN / LEFT ASOF JOIN</text>
  <text x="710" y="114" text-anchor="middle" font-size="10" fill="#64748b">Matches Latest Reference Reading</text>
  <text x="710" y="134" text-anchor="middle" font-size="10" fill="#64748b">Condition: s.ts &gt;= r.ts</text>
  <text x="710" y="154" text-anchor="middle" font-size="10" fill="#059669">Zero Window Cross-Product Overhead</text>
</svg>

