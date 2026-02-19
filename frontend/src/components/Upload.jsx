import { useRef, useState } from "react";

const REQUIRED_COLUMNS = [
  "transaction_id",
  "sender_id",
  "receiver_id",
  "amount",
  "timestamp",
];
const TIMESTAMP_REGEX = /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/;

const SAMPLE_CSV = `transaction_id,sender_id,receiver_id,amount,timestamp
TX001,ACC_A,ACC_B,1000.50,2026-01-01 09:00:00
TX002,ACC_B,ACC_C,998.00,2026-01-01 09:20:00
TX003,ACC_C,ACC_A,995.20,2026-01-01 09:40:00`;

const CANDIDATE_DELIMITERS = [",", ";", "\t", "|"];

const unwrapQuotedLine = (line) => {
  const raw = String(line || "").replace(/^\uFEFF/, "").trim();
  if (raw.length >= 2 && raw.startsWith('"') && raw.endsWith('"')) {
    return raw.slice(1, -1);
  }
  return raw;
};

const detectDelimiter = (line) => {
  const text = unwrapQuotedLine(line);
  let selected = ",";
  let bestCount = -1;

  for (const delimiter of CANDIDATE_DELIMITERS) {
    const count = text.split(delimiter).length - 1;
    if (count > bestCount) {
      bestCount = count;
      selected = delimiter;
    }
  }

  return selected;
};

const splitDelimitedLine = (line, delimiter) => {
  const source = unwrapQuotedLine(line);
  const result = [];
  let token = "";
  let inQuotes = false;

  for (let i = 0; i < source.length; i += 1) {
    const char = source[i];

    if (char === '"') {
      if (inQuotes && source[i + 1] === '"') {
        token += '"';
        i += 1;
      } else {
        inQuotes = !inQuotes;
      }
      continue;
    }

    if (char === delimiter && !inQuotes) {
      result.push(token.trim());
      token = "";
      continue;
    }

    token += char;
  }

  result.push(token.trim());
  return result;
};

const parseCsvText = (text) => {
  const lines = text
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean);

  if (lines.length < 2) {
    throw new Error("CSV must include a header and at least one data row.");
  }

  const delimiter = detectDelimiter(lines[0]);
  const headers = splitDelimitedLine(lines[0], delimiter);

  const missing = REQUIRED_COLUMNS.filter((col) => !headers.includes(col));
  const extra = headers.filter((col) => !REQUIRED_COLUMNS.includes(col));
  if (
    missing.length > 0 ||
    extra.length > 0 ||
    headers.length !== REQUIRED_COLUMNS.length
  ) {
    throw new Error(
      `Header mismatch. Missing: [${missing.join(", ")}] Extra: [${extra.join(", ")}]`
    );
  }

  const indexByHeader = Object.fromEntries(headers.map((h, i) => [h, i]));
  const seenTxIds = new Set();
  const rows = [];

  for (let i = 1; i < lines.length; i += 1) {
    const values = splitDelimitedLine(lines[i], delimiter);
    if (values.length !== headers.length) {
      throw new Error(`Malformed CSV row at line ${i + 1}.`);
    }

    const transaction_id = values[indexByHeader.transaction_id];
    const sender_id = values[indexByHeader.sender_id];
    const receiver_id = values[indexByHeader.receiver_id];
    const amountRaw = values[indexByHeader.amount];
    const timestamp = values[indexByHeader.timestamp];

    if (!transaction_id || !sender_id || !receiver_id || !timestamp) {
      throw new Error(`Missing required values at line ${i + 1}.`);
    }

    if (seenTxIds.has(transaction_id)) {
      throw new Error(`Duplicate transaction_id '${transaction_id}' found.`);
    }
    seenTxIds.add(transaction_id);

    const amount = Number(amountRaw);
    if (!Number.isFinite(amount)) {
      throw new Error(`Invalid numeric amount at line ${i + 1}.`);
    }

    if (!TIMESTAMP_REGEX.test(timestamp)) {
      throw new Error(
        `Invalid timestamp format at line ${i + 1}. Expected YYYY-MM-DD HH:MM:SS.`
      );
    }

    rows.push({
      transaction_id,
      sender_id,
      receiver_id,
      amount,
      timestamp,
    });
  }

  return rows;
};

function Upload({ onAnalyze, isLoading }) {
  const inputRef = useRef(null);
  const [selectedFile, setSelectedFile] = useState(null);
  const [parsedRows, setParsedRows] = useState([]);
  const [error, setError] = useState("");
  const [dragging, setDragging] = useState(false);

  const handleFile = async (file) => {
    setError("");
    if (!file) return;
    if (!file.name.toLowerCase().endsWith(".csv")) {
      setError("Please upload a .csv file.");
      return;
    }

    try {
      const text = await file.text();
      const rows = parseCsvText(text);
      setSelectedFile(file);
      setParsedRows(rows);
    } catch (err) {
      setSelectedFile(null);
      setParsedRows([]);
      setError(err.message || "Invalid CSV file.");
    }
  };

  const handleSubmit = () => {
    if (!selectedFile || parsedRows.length === 0) {
      setError("Choose a valid CSV before analysis.");
      return;
    }
    onAnalyze(selectedFile, parsedRows);
  };

  const downloadSample = () => {
    const blob = new Blob([SAMPLE_CSV], { type: "text/csv;charset=utf-8;" });
    const url = URL.createObjectURL(blob);
    const anchor = document.createElement("a");
    anchor.href = url;
    anchor.download = "sample_valid.csv";
    anchor.click();
    URL.revokeObjectURL(url);
  };

  return (
    <section className="rounded-xl border border-cyan-300/20 bg-gradient-to-br from-[#111a2f]/90 to-[#112727]/90 p-4 shadow-[0_10px_35px_rgba(34,211,238,0.12)]">
      <div
        className={`rounded-lg border-2 border-dashed p-6 text-center transition ${
          dragging
            ? "border-emerald-300 bg-emerald-500/15"
            : "border-cyan-300/35 bg-cyan-500/5"
        }`}
        onDragOver={(e) => {
          e.preventDefault();
          setDragging(true);
        }}
        onDragLeave={() => setDragging(false)}
        onDrop={(e) => {
          e.preventDefault();
          setDragging(false);
          const file = e.dataTransfer.files?.[0];
          handleFile(file);
        }}
      >
        <p className="font-display text-lg text-cyan-100">Drag and drop CSV here</p>
        <p className="mt-1 text-sm text-slate-300">
          Required columns: transaction_id, sender_id, receiver_id, amount, timestamp
        </p>
        <div className="mt-4 flex flex-wrap items-center justify-center gap-2">
          <button
            type="button"
            onClick={() => inputRef.current?.click()}
            className="rounded-md border border-cyan-300/30 bg-cyan-500/10 px-4 py-2 text-sm font-semibold text-cyan-100 transition hover:bg-cyan-500/20"
          >
            Choose CSV
          </button>
          <button
            type="button"
            onClick={downloadSample}
            className="rounded-md border border-emerald-300/40 bg-emerald-500/10 px-4 py-2 text-sm font-semibold text-emerald-200 transition hover:bg-emerald-500/20"
          >
            Download Sample CSV
          </button>
          <button
            type="button"
            onClick={handleSubmit}
            disabled={isLoading}
            className="rounded-md bg-gradient-to-r from-cyan-300 to-emerald-300 px-4 py-2 text-sm font-semibold text-slate-950 transition hover:brightness-110 disabled:cursor-not-allowed disabled:brightness-75"
          >
            {isLoading ? "Analyzing..." : "Upload and Analyze"}
          </button>
        </div>

        <input
          ref={inputRef}
          type="file"
          accept=".csv,text/csv"
          className="hidden"
          onChange={(e) => handleFile(e.target.files?.[0])}
        />

        {selectedFile && (
          <p className="mt-3 text-xs text-cyan-100/90">
            Selected: {selectedFile.name} ({parsedRows.length} rows parsed)
          </p>
        )}
      </div>

      {error && (
        <p className="mt-3 rounded-md border border-rose-300/40 bg-rose-500/15 p-2 text-sm text-rose-100">
          {error}
        </p>
      )}
    </section>
  );
}

export default Upload;
