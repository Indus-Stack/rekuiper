import { fileURLToPath } from 'node:url';
import { defineConfig } from 'vitepress';
import { withMermaid } from 'vitepress-plugin-mermaid';
import directory from '../directory.json';

interface DirectoryItem {
  title: string;
  path?: string;
  collapsed?: boolean;
  children?: DirectoryItem[];
}

function transformSidebar(items: DirectoryItem[]): any[] {
  return items.map((item) => {
    const entry: any = { text: item.title };
    if (item.path) {
      entry.link = item.path === '.' || item.path === './'
        ? '/'
        : item.path.startsWith('/')
          ? item.path
          : `/${item.path}`;
    }
    if (item.collapsed !== undefined) {
      entry.collapsed = item.collapsed;
    }
    if (item.children && item.children.length > 0) {
      entry.items = transformSidebar(item.children);
    }
    return entry;
  });
}

export default withMermaid(
  defineConfig({
  title: 'rekuiper',
  titleTemplate: ':title — Rust Edge SQL Stream Processing Engine',
  description: 'Pure-Rust, drop-in compatible rewrite of LF Edge eKuiper. Process 1,000,000–2,200,000+ IoT, MQTT, Modbus, and OPC-UA events/sec on a single core in <50 MB RAM with native GitOps, Wasm, and MCP.',
  srcDir: './en_US',
  outDir: './.vitepress/dist',
  cleanUrls: false,
  ignoreDeadLinks: true,
  sitemap: {
    hostname: 'https://rekuiper.indramqtt.com'
  },
  markdown: {
    theme: {
      light: 'github-dark',
      dark: 'github-dark'
    }
  },
  vite: {
    resolve: {
      alias: [
        {
          find: /^fastdom\/extensions\/fastdom-promised(\.js)?$/,
          replacement: fileURLToPath(new URL('./fastdom-promised-shim.mjs', import.meta.url))
        },
        {
          find: /^fastdom$/,
          replacement: fileURLToPath(new URL('./fastdom-shim.mjs', import.meta.url))
        }
      ]
    }
  },

  head: [
    ['meta', { name: 'keywords', content: 'rekuiper, eKuiper alternative, Rust stream processing, edge SQL engine, IoT rule engine, MQTT SQL filter, Modbus OPC-UA edge gateway, MCP server IoT' }],
    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:site_name', content: 'rekuiper' }],
    ['meta', { property: 'og:title', content: 'rekuiper — Edge SQL Stream Processing at Rust Speed' }],
    ['meta', { property: 'og:description', content: '100% eKuiper-compatible SQL rule engine in pure Rust. 1M–2.2M+ events/s on a single core, zero GC pauses, native Modbus/OPC-UA, and built-in MCP server.' }],
    ['meta', { property: 'og:image', content: 'https://rekuiper.indramqtt.com/cover.jpg' }],
    ['meta', { name: 'twitter:card', content: 'summary_large_image' }],
    ['meta', { name: 'twitter:title', content: 'rekuiper — Rust Edge SQL Stream Processing Engine' }],
    ['meta', { name: 'twitter:description', content: 'Filter, aggregate, and route MQTT, Modbus, and OPC-UA streams on the gateway before they hit your cloud bill.' }],
    ['meta', { name: 'twitter:image', content: 'https://rekuiper.indramqtt.com/cover.jpg' }]
  ],
  themeConfig: {
    nav: [
      { text: 'Quick Start', link: '/getting_started/getting_started' },
      {
        text: 'Documentation',
        items: [
          { text: 'Core Concepts & SQL', link: '/concepts/rekuiper' },
          { text: 'Native Connectors', link: '/guide/connector' },
          { text: 'Declarative GitOps Mode', link: '/operation/declarative_gitops' },
          { text: 'Built-in Web UI & Observability', link: '/operation/observability_ui_top' },
          { text: 'REST API & CLI Reference', link: '/api/restapi/overview' },
          { text: 'Interactive OpenAPI / Swagger UI', link: '/api/swagger' }
        ]
      },
      { text: 'AI & MCP', link: '/mcp/overview' },
      { text: 'Benchmarks', link: '/benchmarks/overview' },
      {
        text: 'Compare',
        items: [
          { text: 'rekuiper vs. Go eKuiper', link: '/compare/rekuiper-vs-ekuiper' },
          { text: 'rekuiper vs. Node-RED / Vector', link: '/compare/rekuiper-vs-nodered-vector' },
          { text: 'Migration & Modernization Guide', link: '/operation/migration' }
        ]
      },
      {
        text: 'Solutions & Use Cases',
        items: [
          { text: 'Reduce Cloud IoT Costs (95%)', link: '/solutions/reduce-cloud-iot-costs' },
          { text: 'Industrial AI Agents with MCP', link: '/solutions/mcp-industrial-ai-agents' },
          { text: 'Industrial IoT (IIoT)', link: '/usecases/iiot' },
          { text: 'Connected Vehicles & EV Fleets', link: '/usecases/iov' },
          { text: 'ESPHome Fleet Telemetry', link: '/usecases/esphome' }
        ]
      },
      { text: 'IndraMQTT Platform', link: 'https://indramqtt.com' }
    ],
    sidebar: transformSidebar((directory as any).en || []),
    socialLinks: [
      { icon: 'github', link: 'https://github.com/Indus-Stack/rekuiper' }
    ],
    search: {
      provider: 'local'
    },
    footer: {
      message: 'rekuiper is open source (Apache-2.0 / MIT) · For an all-in-one MQTT broker with embedded rekuiper rules, see <a href="https://indramqtt.com" target="_blank" rel="noopener">IndraMQTT</a>.',
      copyright: 'Copyright © 2026 rekuiper contributors'
    }
  }
}));
