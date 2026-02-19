import { useEffect, useMemo, useState } from "react";
import axios from "axios";
import Upload from "./components/Upload";
import GraphView from "./components/GraphView";
import RingTable from "./components/RingTable";

const API_BASE = import.meta.env.VITE_API_BASE_URL || "http://localhost:8000";

const NAV_ITEMS = [
  { label: "Home", id: "home" },
  { label: "About Us", id: "about" },
  { label: "How It Works", id: "how" },
  { label: "Contact", id: "contact" },
];

function App() {
  const [route, setRoute] = useState(window.location.pathname || "/");
  const [analysis, setAnalysis] = useState(null);
  const [transactions, setTransactions] = useState([]);
  const [graphData, setGraphData] = useState({ nodes: [], links: [] });
  const [selectedNode, setSelectedNode] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [menuOpen, setMenuOpen] = useState(false);

  useEffect(() => {
    const onPopState = () => setRoute(window.location.pathname || "/");
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  const navigate = (path) => {
    if (window.location.pathname !== path) {
      window.history.pushState({}, "", path);
      setRoute(path);
    }
    setMenuOpen(false);
    window.scrollTo({ top: 0, behavior: "smooth" });
  };

  const buildGraphData = (rows, result) => {
    const accountSet = new Set();
    for (const row of rows) {
      accountSet.add(row.sender_id);
      accountSet.add(row.receiver_id);
    }

    const suspiciousLookup = new Map(
      result.suspicious_accounts.map((item) => [item.account_id, item])
    );

    const nodes = Array.from(accountSet)
      .sort()
      .map((accountId) => {
        const suspicious = suspiciousLookup.get(accountId);
        return {
          id: accountId,
          suspicion_score: suspicious ? suspicious.suspicion_score : 0,
          detected_patterns: suspicious ? suspicious.detected_patterns : [],
          ring_id: suspicious ? suspicious.ring_id : "",
        };
      });

    const links = rows.map((row) => ({
      source: row.sender_id,
      target: row.receiver_id,
      amount: Number(row.amount),
      transaction_id: row.transaction_id,
      timestamp: row.timestamp,
    }));

    setGraphData({ nodes, links });
  };

  const onAnalyze = async (file, parsedRows) => {
    setError("");
    setLoading(true);
    setSelectedNode(null);
    setTransactions(parsedRows);

    try {
      const formData = new FormData();
      formData.append("file", file);

      const response = await axios.post(`${API_BASE}/analyze`, formData, {
        headers: { "Content-Type": "multipart/form-data" },
        timeout: 60000,
      });

      const result = response.data;
      setAnalysis(result);
      buildGraphData(parsedRows, result);
    } catch (err) {
      const message =
        err?.response?.data?.error ||
        err?.message ||
        "Failed to analyze CSV.";
      setError(message);
      setAnalysis(null);
      setGraphData({ nodes: [], links: [] });
    } finally {
      setLoading(false);
    }
  };

  const selectedTransactions = useMemo(() => {
    if (!selectedNode) return [];
    return transactions
      .filter(
        (tx) =>
          tx.sender_id === selectedNode.id || tx.receiver_id === selectedNode.id
      )
      .sort((a, b) => b.timestamp.localeCompare(a.timestamp))
      .slice(0, 30);
  }, [selectedNode, transactions]);

  const selectedRings = useMemo(() => {
    if (!analysis || !selectedNode) return [];
    return analysis.fraud_rings.filter((ring) =>
      ring.member_accounts.includes(selectedNode.id)
    );
  }, [analysis, selectedNode]);

  const reportText = useMemo(() => {
    if (!analysis) return "";
    const suspicious = analysis.suspicious_accounts;
    const rings = analysis.fraud_rings;
    const patternCounts = suspicious.reduce((acc, row) => {
      for (const pattern of row.detected_patterns) {
        acc[pattern] = (acc[pattern] || 0) + 1;
      }
      return acc;
    }, {});

    const topPatterns = Object.entries(patternCounts)
      .sort((a, b) => b[1] - a[1] || a[0].localeCompare(b[0]))
      .slice(0, 4)
      .map(([name, count]) => `${name} (${count})`)
      .join(", ");

    return [
      `The system analyzed ${analysis.summary.total_accounts_analyzed} accounts and flagged ${analysis.summary.suspicious_accounts_flagged} as suspicious.`,
      `Detected fraud ring count: ${analysis.summary.fraud_rings_detected}. Most common patterns: ${topPatterns || "none"}.`,
      "Accounts were flagged when graph topology and transaction-time behavior matched money muling indicators such as circular routing, fan-in/fan-out concentration, and layered shell chains.",
      "Risk scores are interpreted on a 0-100 scale. Higher values indicate stronger multi-pattern evidence and tighter temporal or structural fraud signatures.",
      "Graph interpretation: directed edges show sender to receiver money flow, suspicious nodes are highlighted, and ring clusters group accounts with shared laundering behavior.",
    ].join(" ");
  }, [analysis]);

  const scrollToSection = (sectionId) => {
    const el = document.getElementById(sectionId);
    if (!el) return;
    el.scrollIntoView({ behavior: "smooth", block: "start" });
    setMenuOpen(false);
  };

  const renderNavbar = () => (
    <header className="sticky top-0 z-50 border-b border-cyan-300/20 bg-[#0b1020]/80 backdrop-blur-xl">
      <div className="mx-auto flex w-full max-w-7xl items-center justify-between px-4 py-3 md:px-8">
        <button
          type="button"
          onClick={() => navigate("/")}
          className="font-display text-2xl font-bold tracking-[0.18em] text-transparent bg-gradient-to-r from-cyan-300 via-teal-200 to-emerald-300 bg-clip-text"
        >
          MULE
        </button>

        <button
          type="button"
          className="rounded-md border border-cyan-300/30 bg-cyan-500/10 px-3 py-1 text-sm text-cyan-100 md:hidden"
          onClick={() => setMenuOpen((value) => !value)}
        >
          Menu
        </button>

        <nav className="hidden items-center gap-6 md:flex">
          {NAV_ITEMS.map((item) => (
            <button
              key={item.id}
              type="button"
              onClick={() =>
                route === "/"
                  ? scrollToSection(item.id)
                  : navigate("/")
              }
              className="text-sm text-slate-200/90 transition hover:text-cyan-200"
            >
              {item.label}
            </button>
          ))}
          <button
            type="button"
            onClick={() => navigate("/get-started")}
            className="rounded-md bg-gradient-to-r from-cyan-300 via-teal-300 to-emerald-300 px-4 py-2 text-sm font-semibold text-slate-950 shadow-[0_6px_25px_rgba(34,211,238,0.35)] transition hover:brightness-110"
          >
            Get Started
          </button>
        </nav>
      </div>

      {menuOpen && (
        <div className="border-t border-cyan-300/20 bg-[#0b1020] px-4 py-3 md:hidden">
          <div className="flex flex-col gap-2">
            {NAV_ITEMS.map((item) => (
              <button
                key={item.id}
                type="button"
                onClick={() =>
                  route === "/"
                    ? scrollToSection(item.id)
                    : navigate("/")
                }
                className="rounded-md border border-cyan-300/20 bg-cyan-500/5 px-3 py-2 text-left text-sm text-slate-200"
              >
                {item.label}
              </button>
            ))}
            <button
              type="button"
              onClick={() => navigate("/get-started")}
              className="rounded-md bg-gradient-to-r from-cyan-300 to-emerald-300 px-3 py-2 text-left text-sm font-semibold text-slate-950"
            >
              Get Started
            </button>
          </div>
        </div>
      )}
    </header>
  );

  const renderLanding = () => (
    <>
      <section
        id="home"
        className="relative overflow-hidden border-b border-cyan-300/20 px-4 py-20 md:px-8 md:py-28"
      >
        <div className="pointer-events-none absolute inset-0 bg-[radial-gradient(circle_at_15%_15%,rgba(20,184,166,0.24),transparent_45%),radial-gradient(circle_at_80%_5%,rgba(59,130,246,0.2),transparent_42%),radial-gradient(circle_at_75%_75%,rgba(251,191,36,0.15),transparent_40%)]" />
        <div className="relative mx-auto w-full max-w-7xl">
          <p className="text-xs uppercase tracking-[0.35em] text-cyan-100/90">
            Graph Financial Intelligence
          </p>
          <h1 className="mt-4 font-display text-5xl font-bold tracking-[0.12em] text-transparent bg-gradient-to-r from-cyan-200 via-teal-100 to-emerald-200 bg-clip-text md:text-7xl">
            MULE
          </h1>
          <p className="mt-4 max-w-3xl text-lg text-slate-100 md:text-xl">
            Graph-Based Financial Crime Detection Engine
          </p>
          <p className="mt-6 max-w-3xl text-sm text-slate-300 md:text-base">
            MULE helps teams detect money muling rings quickly using graph
            structure, temporal behavior, and deterministic risk scoring.
          </p>
          <div className="mt-8 flex flex-wrap gap-3">
            <button
              type="button"
              onClick={() => navigate("/get-started")}
              className="rounded-md bg-gradient-to-r from-cyan-300 via-teal-300 to-emerald-300 px-6 py-3 text-sm font-semibold text-slate-950 shadow-[0_10px_40px_rgba(45,212,191,0.35)] transition hover:brightness-110"
            >
              Get Started
            </button>
            <button
              type="button"
              onClick={() => scrollToSection("about")}
              className="rounded-md border border-cyan-300/40 bg-cyan-400/5 px-6 py-3 text-sm text-cyan-100 transition hover:border-emerald-300 hover:text-emerald-200"
            >
              Explore Platform
            </button>
          </div>
        </div>
      </section>

      <section id="about" className="mx-auto w-full max-w-7xl px-4 py-16 md:px-8">
        <h2 className="font-display text-3xl text-white md:text-4xl">About Us</h2>
        <p className="mt-4 max-w-4xl text-slate-200">
          Money muling is when accounts are used to receive and transfer
          criminal funds to hide the true source. Detecting it is critical
          because fraud rings move fast and create layered transaction trails.
          MULE applies graph theory to map account relationships and identify
          suspicious routing patterns that are difficult to spot manually.
        </p>

        <div className="mt-8 grid gap-4 md:grid-cols-2 xl:grid-cols-5">
          {[
            ["CSV Upload", "Ingest transaction records quickly."],
            ["Graph Build", "Convert accounts and transfers into a directed network."],
            ["Pattern Detection", "Find cycles, fan-in/fan-out, and shell chains."],
            ["Risk Scoring", "Assign deterministic suspicion scores per account."],
            ["JSON Report", "Export exact schema output for audit and submission."],
          ].map(([title, desc]) => (
            <article
              key={title}
              className="rounded-xl border border-cyan-300/20 bg-gradient-to-br from-cyan-500/8 via-blue-500/8 to-emerald-500/10 p-4 shadow-[0_6px_30px_rgba(14,165,233,0.12)] transition duration-300 hover:-translate-y-1 hover:border-emerald-300/40"
            >
              <div className="mb-3 inline-flex h-9 w-9 items-center justify-center rounded-full bg-gradient-to-br from-cyan-300/30 to-emerald-300/30 text-cyan-100">
                ●
              </div>
              <h3 className="text-lg font-semibold text-slate-100">{title}</h3>
              <p className="mt-2 text-sm text-slate-300">{desc}</p>
            </article>
          ))}
        </div>
      </section>

      <section id="how" className="border-y border-cyan-300/20 bg-gradient-to-r from-[#0f172a]/80 via-[#0f1f3a]/70 to-[#10221e]/75">
        <div className="mx-auto w-full max-w-7xl px-4 py-16 md:px-8">
          <h2 className="font-display text-3xl text-white md:text-4xl">
            How It Works
          </h2>
          <div className="mt-8 grid gap-4 md:grid-cols-4">
            {[
              "Upload validated CSV transaction data",
              "Run graph-based fraud ring detection",
              "Inspect interactive graph and ring tables",
              "Download structured JSON findings",
            ].map((step, index) => (
              <div
                key={step}
                className="rounded-xl border border-cyan-300/20 bg-gradient-to-br from-cyan-500/10 to-blue-500/10 p-4"
              >
                <p className="text-xs text-cyan-200">Step {index + 1}</p>
                <p className="mt-2 text-sm text-slate-100">{step}</p>
              </div>
            ))}
          </div>
        </div>
      </section>

      <section id="contact" className="mx-auto w-full max-w-7xl px-4 py-16 md:px-8">
        <h2 className="font-display text-3xl text-white md:text-4xl">Contact</h2>
        <p className="mt-4 text-slate-200">
          Need a walkthrough, custom thresholds, or demo support? Reach out and
          we can help tune MULE for your evaluation data.
        </p>
        <div className="mt-6 rounded-xl border border-emerald-300/30 bg-gradient-to-r from-emerald-500/12 via-cyan-500/10 to-blue-500/12 p-5 text-sm text-emerald-100">
          support@mule-detection.local
        </div>
      </section>
    </>
  );

  const renderDashboard = () => (
    <section className="mx-auto w-full max-w-7xl px-4 py-8 md:px-8">
      <h2 className="font-display text-3xl text-transparent bg-gradient-to-r from-cyan-200 via-sky-100 to-emerald-200 bg-clip-text">Get Started</h2>
      <p className="mt-2 text-sm text-slate-300">
        Upload a transaction CSV and run analysis. Required columns:
        transaction_id, sender_id, receiver_id, amount, timestamp.
      </p>

      <div className="mt-6">
        <Upload onAnalyze={onAnalyze} isLoading={loading} />
      </div>

      {loading && (
        <div className="mt-4 rounded-lg border border-cyan-300/40 bg-gradient-to-r from-cyan-500/20 to-emerald-500/20 p-4 text-sm text-cyan-100">
          <div className="flex items-center gap-2">
            <span className="inline-block h-2 w-2 animate-pulse rounded-full bg-cyan-300" />
            Processing transactions and building fraud intelligence graph...
          </div>
        </div>
      )}

      {error && (
        <div className="mt-4 rounded-lg border border-rose-300/50 bg-rose-500/15 p-4 text-sm text-rose-100">
          {error}
        </div>
      )}

      {analysis && (
        <>
          <section className="mt-6 grid gap-3 sm:grid-cols-2 xl:grid-cols-4">
            <div className="rounded-xl border border-cyan-300/20 bg-gradient-to-br from-cyan-500/12 to-sky-500/8 p-4">
              <p className="text-xs uppercase tracking-wide text-slate-400">
                Accounts analyzed
              </p>
              <p className="mt-1 text-3xl font-semibold text-slate-100">
                {analysis.summary.total_accounts_analyzed}
              </p>
            </div>
            <div className="rounded-xl border border-rose-300/25 bg-gradient-to-br from-rose-500/20 to-orange-500/15 p-4">
              <p className="text-xs uppercase tracking-wide text-red-300/90">
                Suspicious accounts
              </p>
              <p className="mt-1 text-3xl font-semibold text-red-300">
                {analysis.summary.suspicious_accounts_flagged}
              </p>
            </div>
            <div className="rounded-xl border border-amber-300/25 bg-gradient-to-br from-amber-500/20 to-yellow-500/12 p-4">
              <p className="text-xs uppercase tracking-wide text-amber-300/90">
                Fraud rings detected
              </p>
              <p className="mt-1 text-3xl font-semibold text-amber-300">
                {analysis.summary.fraud_rings_detected}
              </p>
            </div>
            <div className="rounded-xl border border-emerald-300/25 bg-gradient-to-br from-emerald-500/20 to-teal-500/15 p-4">
              <p className="text-xs uppercase tracking-wide text-cyan-200/90">
                Processing time
              </p>
              <p className="mt-1 text-3xl font-semibold text-cyan-200">
                {analysis.summary.processing_time_seconds.toFixed(2)}s
              </p>
            </div>
          </section>

          <div className="mt-4 flex flex-wrap gap-3">
            <a
              href={`${API_BASE}/results/download`}
              className="rounded-md bg-gradient-to-r from-cyan-300 to-emerald-300 px-4 py-2 text-sm font-semibold text-slate-950 shadow-[0_8px_30px_rgba(45,212,191,0.3)] transition hover:brightness-110"
            >
              Download Exact JSON
            </a>
            <div className="rounded-md border border-cyan-300/25 bg-cyan-500/8 px-4 py-2 text-xs text-slate-200">
              Legend: <span className="text-red-400">Red nodes</span> suspicious,{" "}
              <span className="text-slate-300">Gray nodes</span> normal.
            </div>
            <div className="rounded-md border border-emerald-300/25 bg-emerald-500/8 px-4 py-2 text-xs text-slate-200">
              Graph supports zoom, pan, and rotate. Click nodes for detail panel.
            </div>
          </div>

          <section className="mt-6 grid gap-4 xl:grid-cols-[2fr_1fr]">
            <div className="rounded-xl border border-cyan-300/20 bg-gradient-to-br from-[#0f1c33]/90 to-[#102322]/90 p-3">
              <GraphView graphData={graphData} onNodeSelect={setSelectedNode} />
            </div>

            <aside className="rounded-xl border border-cyan-300/20 bg-gradient-to-br from-[#111827]/90 to-[#102127]/90 p-4">
              {!selectedNode && (
                <p className="text-sm text-slate-400">
                  Select an account node to inspect transactions, ring
                  membership, and score evidence.
                </p>
              )}

              {selectedNode && (
                <div className="space-y-4">
                  <div>
                    <p className="text-xs uppercase tracking-wide text-slate-400">
                      Account
                    </p>
                    <p className="font-mono text-lg text-slate-100">{selectedNode.id}</p>
                  </div>

                  <div>
                    <p className="text-xs uppercase tracking-wide text-slate-400">
                      Risk score
                    </p>
                    <p className="text-2xl font-semibold text-rose-300">
                      {(selectedNode.suspicion_score || 0).toFixed(2)}
                    </p>
                  </div>

                  <div>
                    <p className="text-xs uppercase tracking-wide text-slate-400">
                      Detected patterns
                    </p>
                    <p className="text-sm text-slate-200">
                      {selectedNode.detected_patterns?.length
                        ? selectedNode.detected_patterns.join(", ")
                        : "None"}
                    </p>
                  </div>

                  <div>
                    <p className="text-xs uppercase tracking-wide text-slate-400">
                      Ring membership
                    </p>
                    {selectedRings.length === 0 && (
                      <p className="text-sm text-slate-400">No ring membership.</p>
                    )}
                    {selectedRings.map((ring) => (
                      <div
                        key={ring.ring_id}
                        className="mt-2 rounded-md border border-cyan-300/20 bg-cyan-500/10 p-2 text-xs"
                      >
                        <p className="font-semibold text-cyan-200">
                          {ring.ring_id} ({ring.pattern_type})
                        </p>
                        <p className="mt-1 text-slate-300">
                          Members: {ring.member_accounts.join(", ")}
                        </p>
                      </div>
                    ))}
                  </div>

                  <div>
                    <p className="text-xs uppercase tracking-wide text-slate-400">
                      Transactions (latest 30)
                    </p>
                      <div className="mt-2 max-h-52 overflow-y-auto rounded-md border border-cyan-300/20 bg-[#071423]/65 p-2">
                      {selectedTransactions.length === 0 && (
                        <p className="text-xs text-slate-400">No transactions.</p>
                      )}
                      {selectedTransactions.map((tx) => (
                        <div
                          key={tx.transaction_id}
                          className="mb-2 rounded border border-cyan-300/20 bg-cyan-500/5 px-2 py-1 text-xs text-slate-300"
                        >
                          <p className="font-mono text-slate-200">{tx.transaction_id}</p>
                          <p>
                            {tx.sender_id} → {tx.receiver_id}
                          </p>
                          <p>
                            Amount: {Number(tx.amount).toFixed(2)} | {tx.timestamp}
                          </p>
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              )}
            </aside>
          </section>

          <section className="mt-6">
            <RingTable rings={analysis.fraud_rings} />
          </section>

          <section className="mt-6 rounded-xl border border-cyan-300/20 bg-gradient-to-br from-[#111a30] to-[#102627] p-5">
            <h3 className="font-display text-2xl text-white">Detailed Report</h3>
            <p className="mt-3 text-sm leading-7 text-slate-300">{reportText}</p>
          </section>
        </>
      )}
    </section>
  );

  return (
    <div className="min-h-screen bg-[radial-gradient(circle_at_10%_12%,rgba(45,212,191,0.18),transparent_35%),radial-gradient(circle_at_85%_8%,rgba(56,189,248,0.18),transparent_35%),radial-gradient(circle_at_80%_75%,rgba(251,191,36,0.12),transparent_35%),linear-gradient(180deg,#060b16_0%,#0b1223_45%,#0a1923_100%)] text-slate-100 font-body">
      {renderNavbar()}
      {route === "/get-started" ? renderDashboard() : renderLanding()}
    </div>
  );
}

export default App;
