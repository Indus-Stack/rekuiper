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
  titleTemplate: ':title — Rust MQTT Rule Engine & Streaming SQL Processor',
  description: 'Single-binary MQTT rule engine and streaming SQL processor written in Rust. Filter, aggregate, and route MQTT, Kafka, Modbus, and OPC-UA data on the edge.',
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
    ['meta', { name: 'keywords', content: 'rekuiper, Rust MQTT rule engine, MQTT rule engine, streaming SQL processor, edge SQL engine, IoT rule engine, Modbus, OPC-UA, stream processing' }],
    ['meta', { property: 'og:type', content: 'website' }],
    ['meta', { property: 'og:site_name', content: 'rekuiper' }],
    ['meta', { property: 'og:title', content: 'rekuiper — Rust MQTT Rule Engine & Streaming SQL Processor' }],
    ['meta', { property: 'og:description', content: 'Single-binary MQTT rule engine and streaming SQL processor written in Rust. Filter, aggregate, and route MQTT, Kafka, Modbus, and OPC-UA data on the edge.' }],
    ['meta', { property: 'og:image', content: 'https://rekuiper.indramqtt.com/cover.jpg' }],
    ['meta', { name: 'twitter:card', content: 'summary_large_image' }],
    ['meta', { name: 'twitter:title', content: 'rekuiper — Rust MQTT Rule Engine & Streaming SQL Processor' }],
    ['meta', { name: 'twitter:description', content: 'Single-binary MQTT rule engine and streaming SQL processor written in Rust. Filter, aggregate, and route MQTT, Kafka, Modbus, and OPC-UA data on the edge.' }],
    ['meta', { name: 'twitter:image', content: 'https://rekuiper.indramqtt.com/cover.jpg' }],
    ['script', { type: 'application/ld+json' }, JSON.stringify({
      "@context": "https://schema.org",
      "@type": "SoftwareApplication",
      "name": "rekuiper",
      "operatingSystem": "Linux, macOS, Windows, ARM64",
      "applicationCategory": "DeveloperApplication",
      "programmingLanguage": "Rust",
      "description": "Single-binary MQTT rule engine and streaming SQL processor written in Rust for edge gateways and telemetry pipelines.",
      "url": "https://rekuiper.indramqtt.com",
      "codeRepository": "https://github.com/Indus-Stack/rekuiper",
      "offers": {
        "@type": "Offer",
        "price": "0",
        "priceCurrency": "USD"
      }
    })]
  ],
  transformHead: ({ pageData }) => {
    const cleanPath = pageData.relativePath === 'index.md'
      ? ''
      : pageData.relativePath.replace(/\.md$/, '.html');
    return [
      ['link', { rel: 'canonical', href: `https://rekuiper.indramqtt.com/${cleanPath}` }]
    ];
  },
  themeConfig: {
    nav: [
      {
        text: 'Docs',
        items: [
          { text: 'Quick Start', link: '/getting_started/getting_started' },
          { text: 'Installation & Deployment', link: '/installation' },
          { text: 'MQTT Rule Engine Architecture', link: '/mqtt-rule-engine' },
          { text: 'Core Concepts & SQL', link: '/concepts/rekuiper' },
          { text: 'Native Connectors', link: '/guide/connector' },
          { text: 'Declarative GitOps Mode', link: '/operation/declarative_gitops' },
          { text: 'Built-in Web UI & Observability', link: '/operation/observability_ui_top' },
          { text: 'Fleet Management & Control Plane', link: '/operation/fleet_management' },
          { text: 'Visual Pipeline Studio', link: '/guide/rules/visual_studio' },
          { text: 'Rule Designer & Schema Builder', link: '/guide/rules/designer_and_schema_builder' },
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
          { text: 'MQTT Rule Engine Landscape', link: '/mqtt-rule-engine#2-architectural-comparison-matrix' },
          { text: 'Migration & Modernization Guide', link: '/operation/migration' }
        ]
      },
      {
        text: 'Solutions',
        items: [
          { text: 'Reduce Cloud IoT Costs (95%)', link: '/solutions/reduce-cloud-iot-costs' },
          { text: 'Industrial AI Agents with MCP', link: '/solutions/mcp-industrial-ai-agents' },
          { text: 'Industrial IoT (IIoT)', link: '/usecases/iiot' },
          { text: 'Connected Vehicles & EV Fleets', link: '/usecases/iov' },
          { text: 'ESPHome Fleet Telemetry', link: '/usecases/esphome' }
        ]
      },
      {
        text: 'More',
        items: [
          { text: 'Frequently Asked Questions (FAQ)', link: '/faq' },
          { text: 'IndraMQTT Platform', link: 'https://indramqtt.com' }
        ]
      }
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
