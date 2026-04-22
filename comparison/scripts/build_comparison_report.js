#!/usr/bin/env node
/**
 * Cross-repo comparison + visualization builder.
 *
 * Inputs:
 *   - <mini-hadoop-dir>/hierarchical-features.json
 *   - <hadoop-dir>/hierarchical-features.json
 *
 * Outputs (in --out):
 *   - cross-repo-analysis.json      -- raw analysis for downstream tooling
 *   - comparison-report.md          -- markdown summary
 *   - comparison-report.html        -- interactive (flowchart + sankey + parity + depgraph)
 *
 * Usage:
 *   node build_comparison_report.js \
 *       --mini <mini-hadoop features dir> \
 *       --hadoop <hadoop features dir> \
 *       --out <output dir>
 */

'use strict';
const fs = require('fs');
const path = require('path');

function parseArgs(argv) {
  const a = { mini: null, hadoop: null, out: null };
  for (let i = 2; i < argv.length; i++) {
    const t = argv[i];
    if (t === '--mini') a.mini = argv[++i];
    else if (t === '--hadoop') a.hadoop = argv[++i];
    else if (t === '--out') a.out = argv[++i];
  }
  if (!a.mini || !a.hadoop || !a.out) {
    console.error('usage: --mini <dir> --hadoop <dir> --out <dir>');
    process.exit(2);
  }
  return a;
}

const args = parseArgs(process.argv);
fs.mkdirSync(args.out, { recursive: true });

const mini = JSON.parse(fs.readFileSync(path.join(args.mini, 'hierarchical-features.json'), 'utf8'));
const hadoop = JSON.parse(
  fs.readFileSync(path.join(args.hadoop, 'hierarchical-features.json'), 'utf8')
);

// ---------------------------------------------------------------------------
// Normalize modules into a common shape for comparison
// ---------------------------------------------------------------------------

/**
 * Extract a short canonical name from a Hadoop-style packagePath by trimming
 * the repeated "/src/main/java/..." tail. E.g.
 *   "hadoop-hdfs-project/hadoop-hdfs/src/main/java/org/apache/hadoop"
 *      -> "hadoop-hdfs-project/hadoop-hdfs"
 *   "hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager/src/main/java/org"
 *      -> "hadoop-yarn-project/hadoop-yarn/hadoop-yarn-server/hadoop-yarn-server-resourcemanager"
 * Leaves Go paths like "pkg/namenode" unchanged.
 */
function shortPath(pkgPath) {
  const norm = (pkgPath || '').replace(/\\/g, '/');
  const idx = norm.search(/\/src\/(main|test)\//);
  if (idx >= 0) return norm.slice(0, idx);
  return norm;
}

function lastSegments(pkgPath, n) {
  const parts = shortPath(pkgPath).split('/').filter(Boolean);
  return parts.slice(-n).join('/');
}

function normalize(fm, repo) {
  const mods = (fm.modules || []).map((m) => {
    // Tokens from file paths within this module — used for detecting
    // subsystem keywords inside sub-packages (e.g., "namenode" inside
    // hadoop-hdfs-project/hadoop-hdfs/.../server/namenode/...).
    const fileTokens = new Map(); // token -> count
    for (const f of m.files || []) {
      const toks = String(f).toLowerCase().split(/[^a-z0-9]+/).filter((t) => t && t.length > 2);
      for (const t of toks) fileTokens.set(t, (fileTokens.get(t) || 0) + 1);
    }
    return {
      id: m.id,
      repo,
      name: m.name_heuristic || m.id,
      packagePath: m.packagePath || m.id,
      shortPath: shortPath(m.packagePath || m.id),
      tail: lastSegments(m.packagePath || m.id, 1),
      tail2: lastSegments(m.packagePath || m.id, 2),
      fileCount: m.metrics?.fileCount ?? m.files?.length ?? 0,
      functionCount: m.metrics?.functionCount ?? 0,
      classCount: m.metrics?.classCount ?? 0,
      linesOfCode: m.metrics?.linesOfCode ?? 0,
      cohesion: m.metrics?.cohesion ?? 0,
      coupling: m.metrics?.coupling ?? 0,
      incomingApiCalls: m.metrics?.incomingApiCalls ?? 0,
      outgoingApiCalls: m.metrics?.outgoingApiCalls ?? 0,
      publicApi: (m.publicApi || []).map((p) => p.name),
      domainId: m.domainId,
      entryPointTypes: (m.entryPoints || []).map((ep) => ep.type),
      fileTokens,
      raw: m,
    };
  });
  const domains = (fm.domains || []).map((d) => ({
    id: d.id,
    repo,
    name: d.name_heuristic,
    moduleIds: d.moduleIds,
    metrics: d.metrics,
  }));
  // L1 function groups — previously discarded. Kept here so the hierarchy
  // emitter can walk Domain → Module → FunctionGroup.
  const functionGroups = (fm.functionGroups || []).map((g) => ({
    id: g.id,
    repo,
    name: g.name_heuristic || g.id,
    moduleId: g.moduleId,
    nodeCount: g.metrics?.nodeCount ?? (g.nodeIds?.length ?? 0),
    cohesion: g.metrics?.cohesion ?? 0,
    coupling: g.metrics?.coupling ?? 0,
    entryPointCount: (g.entryPoints || []).length,
  }));
  const apiSurface = (fm.apiSurface || []).map((c) => ({
    from: c.fromModuleId,
    to: c.toModuleId,
    totalCalls: c.totalCalls || 0,
    edgeTypes: c.edgeTypes || [],
    criticality: c.criticality,
    repo,
  }));
  return { mods, domains, functionGroups, apiSurface, metrics: fm.metrics };
}

const M = normalize(mini, 'mini-hadoop');
const H = normalize(hadoop, 'hadoop');

// ---------------------------------------------------------------------------
// Module matching: fuzzy name + content overlap
// ---------------------------------------------------------------------------

// Known canonical mapping keywords for Hadoop subsystems
const LAYER_MAP = [
  { layer: 'Storage', keywords: ['hdfs', 'block', 'datanode', 'namenode', 'fs', 'storage'] },
  { layer: 'Resources', keywords: ['yarn', 'resourcemanager', 'nodemanager', 'scheduler', 'container'] },
  { layer: 'Compute', keywords: ['mapreduce', 'mrapp', 'jobclient', 'shuffle', 'job', 'dagengine', 'task'] },
  { layer: 'RPC/Common', keywords: ['rpc', 'common', 'config', 'ipc', 'proto', 'record', 'util'] },
  { layer: 'Client/CLI', keywords: ['client', 'cli', 'cmd', 'tool'] },
];

function classifyLayer(packagePath, name) {
  const text = `${packagePath} ${name}`.toLowerCase();
  for (const { layer, keywords } of LAYER_MAP) {
    if (keywords.some((k) => text.includes(k))) return layer;
  }
  return 'Other';
}

// attach layer to each module
for (const m of M.mods) m.layer = classifyLayer(m.packagePath, m.name);
for (const m of H.mods) m.layer = classifyLayer(m.packagePath, m.name);

// Compute pairwise similarity between a mini module and a hadoop module
function tokenize(str) {
  return new Set(
    String(str || '')
      .toLowerCase()
      .split(/[^a-z0-9]+/)
      .filter((t) => t && t.length > 1)
  );
}

function jaccard(a, b) {
  if (a.size === 0 && b.size === 0) return 0;
  let inter = 0;
  for (const t of a) if (b.has(t)) inter++;
  const union = a.size + b.size - inter;
  return union > 0 ? inter / union : 0;
}

// Strong subsystem keywords — if both mini and hadoop module names contain the
// same keyword, give a strong match score. This bridges the vocabulary gap
// between mini "pkg/namenode" and hadoop's `.../hadoop-hdfs/src/main/java/...`
// where "namenode" is part of the subsystem name.
const SUBSYSTEM_KEYS = [
  'namenode', 'datanode', 'resourcemanager', 'nodemanager', 'hdfs', 'yarn',
  'mapreduce', 'common', 'rpc', 'config', 'block', 'dagengine', 'scheduler',
  'timeline', 'client', 'router', 'registry', 'distcp',
];

// Synonyms: when mini uses one term, also look for these related tokens in
// the Hadoop module's file paths.
const KEYWORD_SYNONYMS = {
  rpc: ['rpc', 'ipc', 'protocolpb', 'protobuf'],
  config: ['config', 'conf', 'configuration'],
  proto: ['proto', 'protocolpb', 'protobuf'],
  cmd: ['cli', 'shell', 'tools'],
  dagengine: ['dag', 'mrapp', 'task', 'job'],
  block: ['block', 'blockmanagement'],
};

function subsystemHits(tokensA, tokensB) {
  let hits = 0;
  for (const k of SUBSYSTEM_KEYS) {
    if (tokensA.has(k) && tokensB.has(k)) hits++;
  }
  return hits;
}

function matchScore(mini, hadoop) {
  if (mini.layer !== hadoop.layer) return -1;
  const ta = tokenize(`${mini.name} ${mini.shortPath} ${mini.tail} ${mini.tail2} ${mini.publicApi.join(' ')}`);
  const tb = tokenize(`${hadoop.name} ${hadoop.shortPath} ${hadoop.tail} ${hadoop.tail2} ${hadoop.publicApi.join(' ')}`);
  let score = jaccard(ta, tb);
  // Strong subsystem keyword alignment
  const hits = subsystemHits(ta, tb);
  if (hits > 0) score += 0.3 * hits;
  // Direct tail equality bonus
  if (mini.tail && hadoop.tail && mini.tail === hadoop.tail) score += 0.4;
  // Mini tail appearing anywhere in hadoop shortPath
  if (mini.tail && hadoop.shortPath.toLowerCase().includes(mini.tail.toLowerCase())) score += 0.25;
  // Subsystem keyword also present in the Hadoop module's file paths (strong signal
  // when the keyword shows up deep inside packages, e.g. namenode inside hadoop-hdfs).
  // Weighted by absolute count AND coverage so tiny match-everything modules don't win.
  if (hadoop.fileTokens && hadoop.fileCount >= 5) {
    const miniTailLc = (mini.tail || '').toLowerCase();
    const tailSynonyms = KEYWORD_SYNONYMS[miniTailLc] || [miniTailLc];
    let tailHitCount = 0;
    for (const syn of tailSynonyms) {
      tailHitCount += hadoop.fileTokens.get(syn) || 0;
    }
    if (tailHitCount > 0) {
      const coverage = tailHitCount / hadoop.fileCount;
      // log10 absolute count dominates so "222 hits in 716 files" (hdfs) beats
      // "10 hits in 10 files" (fs2img) even when fs2img has 100% coverage.
      const absScore = Math.log10(1 + tailHitCount) * 0.6; // 0 .. ~1.4 for big modules
      score += Math.min(0.4, coverage * 0.5) + absScore;
    }
    for (const k of SUBSYSTEM_KEYS) {
      if (!ta.has(k)) continue;
      const syns = KEYWORD_SYNONYMS[k] || [k];
      let hitCount = 0;
      for (const syn of syns) hitCount += hadoop.fileTokens.get(syn) || 0;
      if (hitCount > 0) {
        const coverage = hitCount / hadoop.fileCount;
        score += Math.min(0.2, coverage * 0.5) + Math.min(0.3, Math.log2(1 + hitCount) / 12);
      }
    }
  }
  return score;
}

// For each mini module, find top-3 Hadoop matches and the best match if above threshold
const MATCH_THRESHOLD = 0.12;
const matches = [];
for (const m of M.mods) {
  const scored = H.mods
    .map((h) => ({ h, score: matchScore(m, h) }))
    .filter((x) => x.score >= 0)
    .sort((a, b) => b.score - a.score);
  const top3 = scored.slice(0, 3);
  const best = top3[0] && top3[0].score >= MATCH_THRESHOLD ? top3[0] : null;
  matches.push({
    miniModuleId: m.id,
    miniModuleName: m.name,
    miniLayer: m.layer,
    miniPackage: m.packagePath,
    miniFunctionCount: m.functionCount,
    miniLinesOfCode: m.linesOfCode,
    bestHadoop: best ? { id: best.h.id, name: best.h.name, packagePath: best.h.packagePath, score: Number(best.score.toFixed(3)), fns: best.h.functionCount, loc: best.h.linesOfCode } : null,
    candidates: top3.map((x) => ({
      id: x.h.id,
      packagePath: x.h.packagePath,
      score: Number(x.score.toFixed(3)),
    })),
  });
}

// Reverse match: find which Hadoop modules are NOT covered by mini
const coveredHadoopIds = new Set(
  matches.filter((x) => x.bestHadoop).map((x) => x.bestHadoop.id)
);
const uncoveredHadoop = H.mods.filter((h) => !coveredHadoopIds.has(h.id));

// By-layer aggregates
function byLayer(mods) {
  const acc = {};
  for (const m of mods) {
    if (!acc[m.layer]) acc[m.layer] = { modules: 0, functions: 0, loc: 0, files: 0, names: [] };
    acc[m.layer].modules += 1;
    acc[m.layer].functions += m.functionCount;
    acc[m.layer].loc += m.linesOfCode;
    acc[m.layer].files += m.fileCount;
    acc[m.layer].names.push(m.name);
  }
  return acc;
}

const layerMini = byLayer(M.mods);
const layerHadoop = byLayer(H.mods);

const analysis = {
  generatedAt: new Date().toISOString(),
  mini: {
    repo: 'mini-hadoop',
    totalModules: M.metrics?.totalModules ?? M.mods.length,
    totalDomains: M.metrics?.totalDomains ?? M.domains.length,
    totalFunctionGroups: M.metrics?.totalFunctionGroups ?? 0,
    totalLinesOfCode: M.metrics?.totalLinesOfCode ?? 0,
    totalFiles: M.metrics?.totalFiles ?? 0,
    totalApiContracts: M.metrics?.totalApiContracts ?? M.apiSurface.length,
  },
  hadoop: {
    repo: 'hadoop',
    totalModules: H.metrics?.totalModules ?? H.mods.length,
    totalDomains: H.metrics?.totalDomains ?? H.domains.length,
    totalFunctionGroups: H.metrics?.totalFunctionGroups ?? 0,
    totalLinesOfCode: H.metrics?.totalLinesOfCode ?? 0,
    totalFiles: H.metrics?.totalFiles ?? 0,
    totalApiContracts: H.metrics?.totalApiContracts ?? H.apiSurface.length,
  },
  layerAggregates: {
    mini: layerMini,
    hadoop: layerHadoop,
  },
  matches,
  uncoveredHadoop: uncoveredHadoop.map((h) => ({
    id: h.id,
    layer: h.layer,
    packagePath: h.packagePath,
    functionCount: h.functionCount,
    linesOfCode: h.linesOfCode,
  })),
  miniModulesByLayer: Object.fromEntries(
    LAYER_MAP.concat([{ layer: 'Other' }]).map(({ layer }) => [
      layer,
      M.mods.filter((m) => m.layer === layer).map(({ raw, fileTokens, ...rest }) => rest),
    ])
  ),
  hadoopModulesByLayer: Object.fromEntries(
    LAYER_MAP.concat([{ layer: 'Other' }]).map(({ layer }) => [
      layer,
      H.mods.filter((m) => m.layer === layer).map(({ raw, fileTokens, ...rest }) => rest),
    ])
  ),
  miniApiContracts: M.apiSurface,
  hadoopApiContracts: H.apiSurface,
  // Feature hierarchy (Repo → Domain → Module → FunctionGroup). Previously
  // thrown away by the layer flattening. Consumers wanting an agent-
  // traversable view should read from these three fields instead of
  // `*ModulesByLayer` which collapses to 6 keyword-matched buckets.
  miniHierarchy: {
    domains: M.domains,
    functionGroups: M.functionGroups,
  },
  hadoopHierarchy: {
    domains: H.domains,
    functionGroups: H.functionGroups,
  },
};

fs.writeFileSync(path.join(args.out, 'cross-repo-analysis.json'), JSON.stringify(analysis, null, 2));
console.log('[cmp] wrote cross-repo-analysis.json');

// ---------------------------------------------------------------------------
// Markdown report
// ---------------------------------------------------------------------------

// Render one repo's feature hierarchy — Domain → Module → FunctionGroup
// — as a collapsible markdown tree. Deliberately uses `<details>` so the
// report stays readable even on 10K+ module repos.
function renderHierarchy(lines, label, hier, modsById, fgByModule) {
  const safe = (x) => (typeof x === 'number' ? x.toLocaleString() : x ?? '-');
  const domains = hier.domains || [];
  const totalModules = domains.reduce((n, d) => n + (d.moduleIds?.length || 0), 0);
  lines.push(`### ${label} — ${domains.length} domains / ${totalModules} modules`);
  lines.push('');
  // Sort domains by total nodes so the biggest subsystem shows first.
  const sorted = [...domains].sort(
    (a, b) => (b.metrics?.totalNodes || 0) - (a.metrics?.totalNodes || 0),
  );
  for (const d of sorted) {
    const metric = d.metrics || {};
    const dsummary = `${safe(metric.moduleCount ?? d.moduleIds?.length)} modules · ${safe(metric.totalNodes)} nodes · ${safe(metric.totalFiles)} files · internal coupling ${((metric.internalCoupling || 0) * 100).toFixed(0)}%`;
    lines.push(`<details><summary><strong>${d.name || d.id}</strong> — ${dsummary}</summary>`);
    lines.push('');
    for (const modId of d.moduleIds || []) {
      const m = modsById.get(modId);
      if (!m) continue;
      const msummary = `${safe(m.functionCount)} fns · ${safe(m.linesOfCode)} LOC · ${safe(m.fileCount)} files · cohesion ${(m.cohesion * 100).toFixed(0)}%`;
      lines.push(`- <details><summary><code>${m.packagePath}</code> — ${msummary}</summary>`);
      lines.push('');
      const fgs = fgByModule.get(modId) || [];
      if (fgs.length === 0) {
        lines.push('  _(no function groups)_');
      } else {
        for (const g of fgs) {
          lines.push(
            `  - \`${g.name}\` — ${safe(g.nodeCount)} nodes · cohesion ${(g.cohesion * 100).toFixed(0)}% · coupling ${(g.coupling * 100).toFixed(0)}%`,
          );
        }
      }
      lines.push('');
      lines.push('  </details>');
    }
    lines.push('');
    lines.push('</details>');
    lines.push('');
  }
}

function renderMarkdown(a) {
  const lines = [];
  // Lookups used by the hierarchy renderer below.
  const M_hier = a.miniHierarchy || { domains: [], functionGroups: [] };
  const H_hier = a.hadoopHierarchy || { domains: [], functionGroups: [] };
  const M_mods_by_id = new Map(
    (a.miniModulesByLayer
      ? Object.values(a.miniModulesByLayer).flat()
      : []).map((m) => [m.id, m]),
  );
  const H_mods_by_id = new Map(
    (a.hadoopModulesByLayer
      ? Object.values(a.hadoopModulesByLayer).flat()
      : []).map((m) => [m.id, m]),
  );
  const groupFgByModule = (fgs) => {
    const out = new Map();
    for (const g of fgs || []) {
      if (!out.has(g.moduleId)) out.set(g.moduleId, []);
      out.get(g.moduleId).push(g);
    }
    for (const list of out.values())
      list.sort((x, y) => (y.nodeCount || 0) - (x.nodeCount || 0));
    return out;
  };
  const M_fg_by_module = groupFgByModule(M_hier.functionGroups);
  const H_fg_by_module = groupFgByModule(H_hier.functionGroups);

  lines.push('# Mini-Hadoop vs Apache Hadoop — Feature Comparison');
  lines.push('');
  lines.push(`_Generated at ${a.generatedAt}_`);
  lines.push('');
  lines.push('## Overall scale');
  lines.push('');
  lines.push('| Metric | mini-Hadoop | Hadoop | ratio |');
  lines.push('|---|---:|---:|---:|');
  const safe = (x) => (typeof x === 'number' ? x.toLocaleString() : x ?? '-');
  const ratio = (a, b) => (b > 0 ? ((a / b) * 100).toFixed(2) + '%' : '-');
  lines.push(
    `| modules | ${safe(a.mini.totalModules)} | ${safe(a.hadoop.totalModules)} | ${ratio(a.mini.totalModules, a.hadoop.totalModules)} |`
  );
  lines.push(
    `| domains | ${safe(a.mini.totalDomains)} | ${safe(a.hadoop.totalDomains)} | ${ratio(a.mini.totalDomains, a.hadoop.totalDomains)} |`
  );
  lines.push(
    `| function groups | ${safe(a.mini.totalFunctionGroups)} | ${safe(a.hadoop.totalFunctionGroups)} | ${ratio(a.mini.totalFunctionGroups, a.hadoop.totalFunctionGroups)} |`
  );
  lines.push(
    `| lines of code | ${safe(a.mini.totalLinesOfCode)} | ${safe(a.hadoop.totalLinesOfCode)} | ${ratio(a.mini.totalLinesOfCode, a.hadoop.totalLinesOfCode)} |`
  );
  lines.push(
    `| API contracts (cross-module) | ${safe(a.mini.totalApiContracts)} | ${safe(a.hadoop.totalApiContracts)} | ${ratio(a.mini.totalApiContracts, a.hadoop.totalApiContracts)} |`
  );

  lines.push('');
  lines.push('## Feature hierarchy — Repo → Domain → Module → FunctionGroup');
  lines.push('');
  lines.push(
    '_The true hierarchy emitted by `extractHierarchicalFeatures(depth=3)`. Domains are Louvain communities over the module coupling graph; function groups are intra-module cohesion clusters. This is the view an LLM agent should traverse from overview to detail._',
  );
  lines.push('');
  renderHierarchy(lines, 'mini-Hadoop', M_hier, M_mods_by_id, M_fg_by_module);
  lines.push('');
  renderHierarchy(lines, 'Hadoop', H_hier, H_mods_by_id, H_fg_by_module);

  lines.push('');
  lines.push('## Feature coverage by layer (diagnostic — keyword buckets, not the real hierarchy)');
  lines.push('');
  lines.push(
    '_Kept for backward-compat. These 6 layers come from a hand-coded keyword map (`LAYER_MAP`) and collapse the hierarchy above. Useful for matching mini → Hadoop modules but not for agent navigation._',
  );
  lines.push('');
  lines.push('| Layer | mini modules | mini fns | mini LOC | Hadoop modules | Hadoop fns | Hadoop LOC |');
  lines.push('|---|---:|---:|---:|---:|---:|---:|');
  const layers = Object.keys(a.layerAggregates.hadoop).sort();
  for (const L of layers) {
    const m = a.layerAggregates.mini[L] || { modules: 0, functions: 0, loc: 0 };
    const h = a.layerAggregates.hadoop[L] || { modules: 0, functions: 0, loc: 0 };
    lines.push(
      `| ${L} | ${safe(m.modules)} | ${safe(m.functions)} | ${safe(m.loc)} | ${safe(h.modules)} | ${safe(h.functions)} | ${safe(h.loc)} |`
    );
  }

  lines.push('');
  lines.push('## Mini-Hadoop module → Hadoop match');
  lines.push('');
  lines.push('| Mini module | Layer | fns / LOC | Best Hadoop match | score | Hadoop fns / LOC |');
  lines.push('|---|---|---:|---|---:|---:|');
  const shortenHadoop = (p) => (p || '').replace(/\/src\/(main|test)\/.*$/, '');
  for (const m of a.matches.sort((x, y) => (y.miniFunctionCount || 0) - (x.miniFunctionCount || 0))) {
    const best = m.bestHadoop;
    const bh = best ? `\`${shortenHadoop(best.packagePath)}\`` : '_no strong match_';
    const bs = best ? best.score : '-';
    const bfn = best ? `${safe(best.fns)} / ${safe(best.loc)}` : '-';
    lines.push(
      `| \`${m.miniPackage}\` | ${m.miniLayer} | ${safe(m.miniFunctionCount)} / ${safe(m.miniLinesOfCode)} | ${bh} | ${bs} | ${bfn} |`
    );
  }

  lines.push('');
  lines.push('## Hadoop modules NOT covered by mini-Hadoop (top 20 by function count)');
  lines.push('');
  lines.push('| Hadoop module | Layer | fns | LOC |');
  lines.push('|---|---|---:|---:|');
  for (const u of a.uncoveredHadoop.sort((x, y) => y.functionCount - x.functionCount).slice(0, 20)) {
    const p = (u.packagePath || '').replace(/\/src\/(main|test)\/.*$/, '');
    lines.push(`| \`${p}\` | ${u.layer} | ${safe(u.functionCount)} | ${safe(u.linesOfCode)} |`);
  }

  lines.push('');
  lines.push('## Mini-Hadoop cross-module API contracts (call-count > 0, sorted desc)');
  lines.push('');
  lines.push('| From | To | Calls | Criticality |');
  lines.push('|---|---|---:|---|');
  for (const c of a.miniApiContracts
    .filter((c) => (c.totalCalls || 0) > 0)
    .sort((x, y) => (y.totalCalls || 0) - (x.totalCalls || 0))
    .slice(0, 20)) {
    lines.push(`| \`${c.from}\` | \`${c.to}\` | ${safe(c.totalCalls)} | ${c.criticality} |`);
  }

  lines.push('');
  lines.push('## Hadoop cross-module API contracts (top 20 by call count)');
  lines.push('');
  lines.push('| From | To | Calls | Criticality |');
  lines.push('|---|---|---:|---|');
  for (const c of a.hadoopApiContracts
    .filter((c) => (c.totalCalls || 0) > 0)
    .sort((x, y) => (y.totalCalls || 0) - (x.totalCalls || 0))
    .slice(0, 20)) {
    const fromShort = c.from.replace(/\/src\/(main|test)\/.*$/, '');
    const toShort = c.to.replace(/\/src\/(main|test)\/.*$/, '');
    lines.push(`| \`${fromShort}\` | \`${toShort}\` | ${safe(c.totalCalls)} | ${c.criticality} |`);
  }

  lines.push('');
  lines.push('## Notes on methodology');
  lines.push('');
  lines.push('- Both graphs were produced by project-specific exporters into the `graph-code-indexing` schema:');
  lines.push('  - **mini-Hadoop**: Go AST exporter (`comparison/scripts/go_graph_exporter.go`) using `go/ast`');
  lines.push('  - **Hadoop**: Java AST exporter (`comparison/scripts/java_graph_exporter.js`) using `tree-sitter-java` via `web-tree-sitter`');
  lines.push('- Feature extraction used `graph-code-indexing/dist/features/v2/extractHierarchicalFeatures` at depth 3, no LLM enrichment.');
  lines.push('- Synthetic per-package markers were injected before feature extraction so Go/Java packages map 1:1 to modules.');
  lines.push('- Layer classification uses keyword matching on module name + package path (Storage / Resources / Compute / RPC-Common / Client-CLI / Other).');
  lines.push('- Module matching uses a Jaccard token similarity with layer constraint; threshold = 0.12.');
  return lines.join('\n');
}

fs.writeFileSync(path.join(args.out, 'comparison-report.md'), renderMarkdown(analysis));
console.log('[cmp] wrote comparison-report.md');

// ---------------------------------------------------------------------------
// Interactive HTML report
// ---------------------------------------------------------------------------

function slimForEmbed(a) {
  const slimMod = (m) => ({
    id: m.id,
    name: m.name,
    packagePath: m.packagePath,
    shortPath: m.shortPath,
    layer: m.layer,
    functionCount: m.functionCount,
    linesOfCode: m.linesOfCode,
    fileCount: m.fileCount,
  });
  const slimContract = (c) => ({
    from: c.from,
    to: c.to,
    totalCalls: c.totalCalls,
    criticality: c.criticality,
  });
  const miniMods = Object.fromEntries(
    Object.entries(a.miniModulesByLayer).map(([k, v]) => [k, v.map(slimMod)])
  );
  const hadoopMods = Object.fromEntries(
    Object.entries(a.hadoopModulesByLayer).map(([k, v]) => [k, v.map(slimMod)])
  );
  return {
    generatedAt: a.generatedAt,
    mini: a.mini,
    hadoop: a.hadoop,
    layerAggregates: a.layerAggregates,
    matches: a.matches,
    uncoveredHadoop: a.uncoveredHadoop,
    miniModulesByLayer: miniMods,
    hadoopModulesByLayer: hadoopMods,
    miniApiContracts: a.miniApiContracts.map(slimContract),
    hadoopApiContracts: a.hadoopApiContracts
      .filter((c) => (c.totalCalls || 0) >= 50)
      .map(slimContract),
  };
}

function renderHtml(a) {
  const slim = slimForEmbed(a);
  const payload = JSON.stringify(slim).replace(/<\/script>/g, '<\\/script>');
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>mini-Hadoop vs Hadoop — Feature Comparison</title>
<script src="https://d3js.org/d3.v7.min.js"></script>
<script src="https://unpkg.com/d3-sankey@0.12.3/dist/d3-sankey.min.js"></script>
<style>
  html, body { margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", system-ui, sans-serif; color: #222; }
  h1 { margin: 0; padding: 18px 24px 6px; font-size: 22px; }
  h2 { margin: 12px 0 4px; padding: 0 24px; font-size: 18px; color: #333; }
  p.lede { padding: 0 24px; color: #666; margin: 4px 0 12px; font-size: 13px; }
  .container { padding: 0 24px 48px; }
  .card { border: 1px solid #e5e7eb; border-radius: 6px; padding: 18px; margin: 16px 0; background: #fff; box-shadow: 0 1px 0 rgba(0,0,0,0.02); }
  .flex { display: flex; gap: 24px; flex-wrap: wrap; }
  .col { flex: 1; min-width: 360px; }
  table { border-collapse: collapse; font-size: 12px; width: 100%; }
  th, td { border: 1px solid #e5e7eb; padding: 4px 8px; text-align: left; }
  th { background: #f9fafb; }
  td.num { text-align: right; font-variant-numeric: tabular-nums; }
  .layer-band { border: 1px solid #e5e7eb; border-radius: 6px; padding: 10px; margin: 8px 0; background: #fafbfc; }
  .layer-band h3 { margin: 0 0 6px 0; font-size: 14px; }
  .layer-bar { height: 10px; background: linear-gradient(90deg,#e5e7eb,#e5e7eb); border-radius: 3px; position: relative; margin-bottom: 6px; }
  .module-chip { display: inline-block; padding: 3px 6px; border-radius: 3px; margin: 2px 2px; font-size: 11px; border: 1px solid #e5e7eb; background: #fff; }
  .module-chip.implemented { background: #d1fae5; border-color: #10b981; }
  .module-chip.partial { background: #fef3c7; border-color: #f59e0b; }
  .module-chip.missing { background: #fee2e2; border-color: #ef4444; }
  .legend { font-size: 12px; margin: 6px 24px; color: #666; }
  .legend .sw { display: inline-block; width: 12px; height: 12px; border-radius: 2px; margin-right: 4px; vertical-align: middle; }
  .sw.implemented { background: #d1fae5; border: 1px solid #10b981; }
  .sw.partial { background: #fef3c7; border: 1px solid #f59e0b; }
  .sw.missing { background: #fee2e2; border: 1px solid #ef4444; }
  .flowchart svg { width: 100%; height: auto; display: block; }
  #parity { font-size: 11px; }
  #parity th, #parity td { padding: 2px 4px; }
  #parity td.cell { text-align: center; min-width: 40px; }
  #depgraph { width: 100%; height: 560px; border: 1px solid #e5e7eb; border-radius: 6px; }
  #sankey svg { width: 100%; height: 440px; }
  .tooltip { position: absolute; background: #111; color: #fff; padding: 6px 8px; border-radius: 4px; font-size: 11px; pointer-events: none; opacity: 0; transition: opacity 120ms; max-width: 280px; }
  code { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; font-size: 11px; }
</style>
</head>
<body>
<h1>mini-Hadoop vs Apache Hadoop — feature pipeline comparison</h1>
<p class="lede">Built from per-repo graph indexing and feature extraction; no LLM enrichment.</p>

<div class="container">
  <div class="card">
    <h2>Scale</h2>
    <table id="scale"></table>
  </div>

  <div class="card">
    <h2>1. Layered dataflow flowchart</h2>
    <p class="lede">
      Hadoop's functional layers, with Hadoop modules (top of each band) and mini-Hadoop equivalents (bottom).
      Green = implemented in mini-Hadoop, yellow = partial match, red = Hadoop module with no mini-Hadoop counterpart.
    </p>
    <div class="legend">
      <span class="sw implemented"></span> implemented &nbsp; &nbsp;
      <span class="sw partial"></span> partial &nbsp; &nbsp;
      <span class="sw missing"></span> missing in mini-Hadoop
    </div>
    <div id="flowchart"></div>
  </div>

  <div class="card">
    <h2>2. Side-by-side Sankey: Domain → Module → Function group</h2>
    <p class="lede">
      Flow widths proportional to LOC. Left: mini-Hadoop. Right: Hadoop.
      Long flows in Hadoop without a mini-Hadoop equivalent indicate unimplemented depth.
    </p>
    <div class="flex">
      <div class="col"><h3 style="margin-left:0">mini-Hadoop</h3><div id="sankey-mini"></div></div>
      <div class="col"><h3 style="margin-left:0">Hadoop (top 30 modules)</h3><div id="sankey-hadoop"></div></div>
    </div>
  </div>

  <div class="card">
    <h2>3. Parity matrix (mini rows × Hadoop columns, top-3 matches each)</h2>
    <p class="lede">Cell shading = Jaccard similarity of module public API surface + package tokens. Top-3 Hadoop matches shown per mini module.</p>
    <div style="overflow-x:auto"><table id="parity"></table></div>
  </div>

  <div class="card">
    <h2>4. Module dependency graph (mini-Hadoop API contracts)</h2>
    <p class="lede">Force-directed graph of mini-Hadoop modules and their cross-module API contracts. Edge weight = calls. Color = layer.</p>
    <div id="depgraph"></div>
  </div>
</div>

<div class="tooltip" id="tt"></div>

<script>
const DATA = ${payload};

// -------- Scale --------
(function() {
  const el = document.getElementById('scale');
  const rows = [
    ['modules', DATA.mini.totalModules, DATA.hadoop.totalModules],
    ['domains', DATA.mini.totalDomains, DATA.hadoop.totalDomains],
    ['function groups', DATA.mini.totalFunctionGroups, DATA.hadoop.totalFunctionGroups],
    ['lines of code', DATA.mini.totalLinesOfCode, DATA.hadoop.totalLinesOfCode],
    ['cross-module API contracts', DATA.mini.totalApiContracts, DATA.hadoop.totalApiContracts],
  ];
  let h = '<tr><th>metric</th><th class=num>mini-Hadoop</th><th class=num>Hadoop</th><th class=num>mini/hadoop</th></tr>';
  const fmt = (n) => typeof n === 'number' ? n.toLocaleString() : '-';
  const pct = (a, b) => b > 0 ? ((a / b) * 100).toFixed(2) + '%' : '-';
  for (const [k, a, b] of rows) {
    h += \`<tr><td>\${k}</td><td class=num>\${fmt(a)}</td><td class=num>\${fmt(b)}</td><td class=num>\${pct(a, b)}</td></tr>\`;
  }
  el.innerHTML = h;
})();

// -------- Flowchart --------
(function() {
  const LAYERS = ['Client/CLI', 'Compute', 'Resources', 'Storage', 'RPC/Common', 'Other'];
  const miniByLayer = {};
  LAYERS.forEach(L => miniByLayer[L] = DATA.miniModulesByLayer[L] || []);
  const hadoopByLayer = {};
  LAYERS.forEach(L => hadoopByLayer[L] = (DATA.hadoopModulesByLayer[L] || []).sort((a,b) => (b.functionCount||0) - (a.functionCount||0)));

  const el = document.getElementById('flowchart');
  const width = Math.max(el.clientWidth || 1000, 1000);
  const rowHeight = 96;
  const totalHeight = LAYERS.length * rowHeight + 36;
  const svg = d3.select(el).append('svg')
      .attr('viewBox', \`0 0 \${width} \${totalHeight}\`)
      .attr('class', 'flowchart');

  // Layer bands
  const bandX = 180;
  const bandWidth = width - bandX - 20;

  // Title row
  svg.append('text').attr('x', bandX).attr('y', 16).attr('font-size', 13).attr('fill', '#333').text('Hadoop modules (sized by fn count, top 8 per layer)');
  svg.append('text').attr('x', 8).attr('y', 36).attr('font-size', 12).attr('fill', '#555').text('Layer');
  svg.append('text').attr('x', bandX).attr('y', 36).attr('font-size', 11).attr('fill', '#888').text('Hadoop (top) / mini-Hadoop (bottom)');

  LAYERS.forEach((L, idx) => {
    const y = 48 + idx * rowHeight;
    const miniModsCovered = DATA.matches.filter(m => m.miniLayer === L && m.bestHadoop);
    const coveredHadoopIds = new Set(miniModsCovered.map(m => m.bestHadoop.id));
    const layerFnsH = (hadoopByLayer[L] || []).reduce((a,m) => a + (m.functionCount||0), 0);
    const layerFnsM = (miniByLayer[L] || []).reduce((a,m) => a + (m.functionCount||0), 0);

    // band background
    svg.append('rect').attr('x', 0).attr('y', y).attr('width', width).attr('height', rowHeight - 6).attr('fill', idx % 2 ? '#f9fafb' : '#ffffff');
    svg.append('text').attr('x', 10).attr('y', y + 22).attr('font-weight', 600).attr('font-size', 13).text(L);
    svg.append('text').attr('x', 10).attr('y', y + 42).attr('font-size', 10).attr('fill', '#666').text(\`H: \${layerFnsH.toLocaleString()} fns\`);
    svg.append('text').attr('x', 10).attr('y', y + 58).attr('font-size', 10).attr('fill', '#666').text(\`M: \${layerFnsM.toLocaleString()} fns\`);
    if (layerFnsH > 0) {
      const covRatio = miniModsCovered.length / Math.max((hadoopByLayer[L] || []).length, 1);
      svg.append('text').attr('x', 10).attr('y', y + 72).attr('font-size', 10).attr('fill', '#999').text(\`coverage: \${(covRatio*100).toFixed(0)}%\`);
    }

    // Hadoop modules: top 8 by fn count
    const hMods = (hadoopByLayer[L] || []).slice(0, 8);
    if (hMods.length > 0) {
      const totalFns = d3.sum(hMods, m => m.functionCount || 1);
      let cursor = bandX;
      hMods.forEach(m => {
        const w = Math.max(40, (m.functionCount / totalFns) * bandWidth);
        const covered = coveredHadoopIds.has(m.id);
        const color = covered ? '#10b981' : '#ef4444';
        const bg = covered ? '#d1fae5' : '#fee2e2';
        const g = svg.append('g');
        g.append('rect').attr('x', cursor).attr('y', y + 8).attr('width', w - 4).attr('height', 34).attr('fill', bg).attr('stroke', color).attr('rx', 3);
        g.append('text').attr('x', cursor + 6).attr('y', y + 22).attr('font-size', 10).attr('font-weight', 600).text((m.name || '').slice(0, Math.max(6, Math.floor(w / 7))));
        g.append('text').attr('x', cursor + 6).attr('y', y + 34).attr('font-size', 9).attr('fill', '#555').text(\`\${(m.functionCount||0).toLocaleString()} fns, \${(m.linesOfCode||0).toLocaleString()} LOC\`);
        g.on('mouseover', (ev) => tooltip(ev, \`<b>\${m.packagePath}</b><br>fns=\${m.functionCount} LOC=\${m.linesOfCode}<br>covered: \${covered ? 'yes' : 'no'}\`));
        g.on('mouseout', hideTooltip);
        cursor += w;
      });
    } else {
      svg.append('text').attr('x', bandX).attr('y', y + 22).attr('font-size', 11).attr('fill', '#bbb').text('(no modules)');
    }

    // mini-Hadoop modules below
    const mMods = (miniByLayer[L] || []).slice(0, 10);
    if (mMods.length > 0) {
      const totalFnsM = d3.sum(mMods, m => m.functionCount || 1) || 1;
      let cursor = bandX;
      mMods.forEach(m => {
        const w = Math.max(40, (m.functionCount / totalFnsM) * bandWidth);
        const match = DATA.matches.find(x => x.miniModuleId === m.id);
        const matched = match && match.bestHadoop;
        const color = matched ? '#10b981' : '#f59e0b';
        const bg = matched ? '#d1fae5' : '#fef3c7';
        const g = svg.append('g');
        g.append('rect').attr('x', cursor).attr('y', y + 50).attr('width', w - 4).attr('height', 30).attr('fill', bg).attr('stroke', color).attr('rx', 3);
        g.append('text').attr('x', cursor + 6).attr('y', y + 62).attr('font-size', 10).attr('font-weight', 600).text(m.name.slice(0, Math.max(6, Math.floor(w / 7))));
        g.append('text').attr('x', cursor + 6).attr('y', y + 74).attr('font-size', 9).attr('fill', '#555').text(\`\${m.functionCount} fns, \${m.linesOfCode} LOC\`);
        g.on('mouseover', (ev) => tooltip(ev, \`<b>\${m.packagePath}</b><br>fns=\${m.functionCount} LOC=\${m.linesOfCode}<br>match: \${matched ? match.bestHadoop.packagePath : '(none)'}\`));
        g.on('mouseout', hideTooltip);
        cursor += w;
      });
    } else if ((hadoopByLayer[L] || []).length > 0) {
      svg.append('text').attr('x', bandX).attr('y', y + 68).attr('font-size', 11).attr('fill', '#ef4444').text('(mini-Hadoop has no modules in this layer)');
    }
  });
})();

// -------- Sankey --------
function buildSankey(containerId, repoName, analysis, which, topN) {
  const mods = which === 'mini' ? DATA.miniModulesByLayer : DATA.hadoopModulesByLayer;
  const domainSource = which === 'mini' ? 'mini' : 'hadoop';
  const el = document.getElementById(containerId);
  const w = el.clientWidth || 540;
  const h = 440;
  const nodes = [];
  const links = [];
  const nodeIdx = new Map();
  function addNode(id, name, level) {
    if (!nodeIdx.has(id)) {
      nodeIdx.set(id, nodes.length);
      nodes.push({ id, name, level });
    }
    return nodeIdx.get(id);
  }
  // Layer -> module (top N)
  const layers = Object.keys(mods);
  for (const L of layers) {
    const ms = (mods[L] || []).sort((a,b) => (b.linesOfCode || 0) - (a.linesOfCode || 0));
    if (ms.length === 0) continue;
    const srcId = 'L:' + L;
    const srcIdx = addNode(srcId, L, 0);
    let taken = 0;
    for (const m of ms) {
      if (taken >= topN) break;
      const weight = Math.max(1, m.linesOfCode || 1);
      const tgtIdx = addNode('M:' + m.id, m.name, 1);
      links.push({ source: srcIdx, target: tgtIdx, value: weight });
      taken++;
    }
  }
  if (nodes.length === 0) {
    el.innerHTML = '<em>no data</em>';
    return;
  }
  const svg = d3.select(el).append('svg').attr('viewBox', \`0 0 \${w} \${h}\`);
  const sk = d3.sankey().nodeWidth(16).nodePadding(6).extent([[8, 8],[w - 140, h - 8]]);
  const g = { nodes: nodes.map(d => Object.assign({}, d)), links: links.map(d => Object.assign({}, d)) };
  sk(g);
  const color = d3.scaleOrdinal(d3.schemeSet2);
  svg.append('g').selectAll('rect').data(g.nodes).enter().append('rect')
      .attr('x', d => d.x0).attr('y', d => d.y0).attr('width', d => d.x1 - d.x0).attr('height', d => Math.max(1, d.y1 - d.y0))
      .attr('fill', d => color(d.name.split(' ')[0]))
      .on('mouseover', (ev, d) => tooltip(ev, \`<b>\${d.name}</b>\`))
      .on('mouseout', hideTooltip);
  svg.append('g').attr('fill', 'none').attr('stroke-opacity', 0.35).selectAll('path').data(g.links).enter().append('path')
      .attr('d', d3.sankeyLinkHorizontal()).attr('stroke', d => color(d.source.name.split(' ')[0])).attr('stroke-width', d => Math.max(1, d.width));
  svg.append('g').selectAll('text').data(g.nodes).enter().append('text')
      .attr('x', d => d.x1 + 4).attr('y', d => (d.y0 + d.y1) / 2).attr('dy', '0.3em').attr('font-size', 10).text(d => d.name);
}
buildSankey('sankey-mini', 'mini-Hadoop', DATA, 'mini', 15);
buildSankey('sankey-hadoop', 'Hadoop', DATA, 'hadoop', 30);

// -------- Parity matrix --------
(function() {
  const el = document.getElementById('parity');
  // rows = mini modules, cols = top-3 hadoop candidates per row
  const uniqueHadoop = new Map();
  for (const m of DATA.matches) for (const c of m.candidates) uniqueHadoop.set(c.id, c);
  const cols = [...uniqueHadoop.values()].slice(0, 40);
  let h = '<tr><th>mini module</th>';
  for (const c of cols) h += \`<th title="\${c.packagePath}">\${c.packagePath.split(/[./]/).slice(-2).join('/')}</th>\`;
  h += '</tr>';
  for (const m of DATA.matches) {
    h += \`<tr><td><code>\${m.miniPackage}</code></td>\`;
    for (const c of cols) {
      const cand = m.candidates.find(x => x.id === c.id);
      const score = cand ? cand.score : 0;
      const intensity = Math.min(1, score * 2);
      const bg = score > 0.05 ? \`rgba(16,185,129,\${intensity})\` : '#ffffff';
      h += \`<td class=cell style="background:\${bg}">\${score ? score.toFixed(2) : ''}</td>\`;
    }
    h += '</tr>';
  }
  el.innerHTML = h;
})();

// -------- Module dependency graph (mini-Hadoop contracts) --------
(function() {
  const el = document.getElementById('depgraph');
  const allMods = Object.values(DATA.miniModulesByLayer).flat();
  const byId = new Map(allMods.map(m => [m.id, m]));
  const nodes = allMods.map(m => ({ id: m.id, name: m.name, layer: m.layer, fns: m.functionCount }));
  const links = DATA.miniApiContracts
    .filter(c => byId.has(c.from) && byId.has(c.to) && c.from !== c.to)
    .map(c => ({ source: c.from, target: c.to, calls: c.totalCalls || 0, criticality: c.criticality }));
  if (nodes.length === 0) { el.innerHTML = '<em>no data</em>'; return; }
  const w = el.clientWidth || 900;
  const h = 560;
  const svg = d3.select(el).append('svg').attr('viewBox', \`0 0 \${w} \${h}\`);
  const layers = ['Client/CLI','Compute','Resources','Storage','RPC/Common','Other'];
  const color = d3.scaleOrdinal().domain(layers).range(d3.schemeSet2);
  const sim = d3.forceSimulation(nodes)
    .force('link', d3.forceLink(links).id(d => d.id).distance(80).strength(0.4))
    .force('charge', d3.forceManyBody().strength(-220))
    .force('center', d3.forceCenter(w/2, h/2));
  const link = svg.append('g').attr('stroke-opacity', 0.5).selectAll('line').data(links).enter().append('line')
      .attr('stroke', d => d.criticality === 'critical' || d.criticality === 'high' ? '#ef4444' : '#94a3b8')
      .attr('stroke-width', d => 0.8 + Math.min(4, d.calls / 3));
  const node = svg.append('g').selectAll('g').data(nodes).enter().append('g')
      .call(d3.drag()
        .on('start', (ev,d) => { if (!ev.active) sim.alphaTarget(0.3).restart(); d.fx = d.x; d.fy = d.y; })
        .on('drag', (ev,d) => { d.fx = ev.x; d.fy = ev.y; })
        .on('end', (ev,d) => { if (!ev.active) sim.alphaTarget(0); d.fx = null; d.fy = null; }));
  node.append('circle').attr('r', d => 6 + Math.sqrt(d.fns || 1)).attr('fill', d => color(d.layer)).attr('stroke', '#334155').attr('stroke-width', 1);
  node.append('text').text(d => d.name).attr('dx', 10).attr('dy', 4).attr('font-size', 11);
  node.on('mouseover', (ev, d) => tooltip(ev, \`<b>\${d.name}</b><br>layer: \${d.layer}<br>fns: \${d.fns}\`));
  node.on('mouseout', hideTooltip);
  sim.on('tick', () => {
    link.attr('x1', d => d.source.x).attr('y1', d => d.source.y).attr('x2', d => d.target.x).attr('y2', d => d.target.y);
    node.attr('transform', d => \`translate(\${d.x},\${d.y})\`);
  });
})();

// -------- Tooltip helpers --------
const tt = document.getElementById('tt');
function tooltip(ev, html) {
  tt.innerHTML = html;
  tt.style.left = (ev.pageX + 12) + 'px';
  tt.style.top = (ev.pageY + 12) + 'px';
  tt.style.opacity = 1;
}
function hideTooltip() {
  tt.style.opacity = 0;
}

</script>
</body>
</html>
`;
}

fs.writeFileSync(path.join(args.out, 'comparison-report.html'), renderHtml(analysis));
console.log('[cmp] wrote comparison-report.html');
console.log('[cmp] done');
