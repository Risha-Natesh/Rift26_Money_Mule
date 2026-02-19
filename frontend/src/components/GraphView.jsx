import { useEffect, useMemo, useRef, useState } from "react";
import ForceGraph3D from "react-force-graph-3d";

const hashString = (value) => {
  let hash = 2166136261;
  for (let i = 0; i < value.length; i += 1) {
    hash ^= value.charCodeAt(i);
    hash +=
      (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
  }
  return hash >>> 0;
};

const hashToCoords = (accountId) => {
  const h1 = hashString(accountId);
  const h2 = hashString(`${accountId}-y`);
  const h3 = hashString(`${accountId}-z`);
  return {
    x: ((h1 % 1200) - 600) * 0.8,
    y: ((h2 % 1200) - 600) * 0.8,
    z: ((h3 % 1200) - 600) * 0.8,
  };
};

const escapeHtml = (text) =>
  String(text)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");

function GraphView({ graphData, onNodeSelect }) {
  const containerRef = useRef(null);
  const [size, setSize] = useState({ width: 760, height: 560 });

  useEffect(() => {
    if (!containerRef.current || typeof ResizeObserver === "undefined") {
      return undefined;
    }
    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const width = Math.max(320, Math.floor(entry.contentRect.width));
        const height = Math.max(420, Math.floor(entry.contentRect.height));
        setSize({ width, height });
      }
    });
    observer.observe(containerRef.current);
    return () => observer.disconnect();
  }, []);

  const prepared = useMemo(() => {
    const nodes = graphData.nodes.map((node) => {
      const { x, y, z } = hashToCoords(node.id);
      return {
        ...node,
        fx: x,
        fy: y,
        fz: z,
      };
    });
    const links = graphData.links.map((link) => ({ ...link }));
    return { nodes, links };
  }, [graphData]);

  if (!graphData.nodes.length) {
    return (
      <div className="flex h-[560px] items-center justify-center rounded-lg border border-slate-800 bg-slate-950/60 text-sm text-slate-400">
        Graph will appear here after analysis.
      </div>
    );
  }

  return (
    <div ref={containerRef} className="h-[560px] w-full overflow-hidden rounded-lg">
      <ForceGraph3D
        width={size.width}
        height={size.height}
        graphData={prepared}
        backgroundColor="#020617"
        cooldownTicks={0}
        d3AlphaDecay={1}
        enableNodeDrag={false}
        linkDirectionalArrowLength={3.8}
        linkDirectionalArrowRelPos={1}
        linkOpacity={0.35}
        linkColor={() => "rgba(148,163,184,0.45)"}
        nodeColor={(node) =>
          node.suspicion_score > 0 ? "#ff4d4f" : "rgba(148,163,184,0.75)"
        }
        nodeVal={(node) => (node.suspicion_score > 0 ? 8 : 3)}
        nodeLabel={(node) => {
          const patterns =
            node.detected_patterns?.length > 0
              ? node.detected_patterns.join(", ")
              : "none";
          return `<div style="padding:6px">
            <div><b>${escapeHtml(node.id)}</b></div>
            <div>score: ${Number(node.suspicion_score || 0).toFixed(2)}</div>
            <div>patterns: ${escapeHtml(patterns)}</div>
          </div>`;
        }}
        onNodeClick={(node) => onNodeSelect(node)}
      />
    </div>
  );
}

export default GraphView;
