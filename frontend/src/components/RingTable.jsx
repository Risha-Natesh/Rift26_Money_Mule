function RingTable({ rings }) {
  return (
    <div className="rounded-xl border border-cyan-300/20 bg-gradient-to-br from-[#101b33]/90 to-[#13212a]/90 p-4">
      <h2 className="mb-3 font-display text-xl text-cyan-100">Fraud Ring Summary</h2>

      <div className="overflow-x-auto">
        <table className="min-w-full border-collapse text-left text-sm">
          <thead>
            <tr className="border-b border-cyan-300/20 text-cyan-100/90">
              <th className="px-3 py-2">Ring ID</th>
              <th className="px-3 py-2">Pattern Type</th>
              <th className="px-3 py-2">Member Count</th>
              <th className="px-3 py-2">Risk Score</th>
              <th className="px-3 py-2">Member IDs</th>
            </tr>
          </thead>
          <tbody>
            {rings.length === 0 && (
              <tr>
                <td className="px-3 py-3 text-slate-300" colSpan={5}>
                  No rings detected.
                </td>
              </tr>
            )}

            {rings.map((ring) => (
              <tr
                key={ring.ring_id}
                className="border-b border-cyan-300/15 align-top text-slate-200"
              >
                <td className="px-3 py-2 font-mono">{ring.ring_id}</td>
                <td className="px-3 py-2 text-emerald-200">{ring.pattern_type}</td>
                <td className="px-3 py-2">{ring.member_accounts.length}</td>
                <td className="px-3 py-2">{Number(ring.risk_score).toFixed(2)}</td>
                <td className="px-3 py-2 font-mono text-xs">
                  {ring.member_accounts.join(", ")}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

export default RingTable;
