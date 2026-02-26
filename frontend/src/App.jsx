import React, { useEffect, useMemo, useRef, useState } from "react";

function pretty(obj) {
  try {
    return JSON.stringify(obj, null, 2);
  } catch {
    return String(obj);
  }
}

function parseResponseBody(text) {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

function getItemTitle(item) {
  return String(item?.Artikelbeschreibung || item?.Name || item?.title || "Item");
}

function getItemNumber(item) {
  return String(item?.itemNumber || item?.item_number || item?.Artikelnummer || item?.article_number || "-");
}

function formatTime(ts) {
  if (!ts) return "-";
  return new Date(ts).toLocaleString();
}

function summarizeData(data, label) {
  if (Array.isArray(data)) {
    const successCount = data.filter((x) => x?.success === true).length;
    const errorCount = data.filter((x) => x?.success !== true).length;
    return `${label}: success ${successCount}, failed ${errorCount}`;
  }

  if (data && typeof data === "object") {
    if (data.success === true) return `${label}: success`;
    if (typeof data.message === "string" && data.message.trim()) return `${label}: ${data.message}`;
    if (typeof data.status === "string") return `${label}: ${data.status}`;
  }

  return `${label}: done`;
}

export default function App() {
  const [apiBase, setApiBase] = useState("/api");
  const [accountMode, setAccountMode] = useState("jvmoebel");
  const [jsonFiles, setJsonFiles] = useState([]);
  const [sourceFile, setSourceFile] = useState("");
  const [jsonItems, setJsonItems] = useState([]);
  const [selectedIds, setSelectedIds] = useState([]);
  const [jsonTotal, setJsonTotal] = useState(null);
  const [search, setSearch] = useState("");
  const [deleteItemNumbers, setDeleteItemNumbers] = useState("");
  const [loading, setLoading] = useState(false);
  const [rawOutput, setRawOutput] = useState("");
  const [lastActionAt, setLastActionAt] = useState(null);
  const [activeUpdateJobId, setActiveUpdateJobId] = useState("");
  const [connectionOk, setConnectionOk] = useState(false);
  const updatePollTimerRef = useRef(null);
  const [status, setStatus] = useState({
    type: "idle",
    title: "Ready",
    text: "Check API connection first.",
  });

  const endpoints = useMemo(() => {
    const base = apiBase.replace(/\/+$/, "");
    return {
      docs: `${base}/docs`,
      upload: `${base}/items/upload`,
      uploadAsync: `${base}/items/upload_async`,
      update: `${base}/items/update`,
      updateAsync: `${base}/items/update_async`,
      deleteAsyncStatus: `${base}/items/delete_async`,
      status: `${base}/items/status`,
      json: `${base}/items/json`,
      jsonFiles: `${base}/items/json/files`,
      validateOne: `${base}/items/validate_one/`,
      uploadOneBulk: `${base}/items/upload_one`,
      updateOneBulk: `${base}/items/update_one`,
      deleteByItemNumberBulk: `${base}/items/delete/by-item-number`,
      deleteBySourceFile: `${base}/items/delete/by-source-file`,
      deleteBySourceFileAsync: `${base}/items/delete/by-source-file_async`,
      deleteDuplicatesByEan: `${base}/items/delete/duplicates-by-ean`,
      deleteDuplicatesByEanAsync: `${base}/items/delete/duplicates-by-ean_async`,
      deleteAll: `${base}/items/delete/all`,
      deleteAllAsync: `${base}/items/delete/all_async`,
    };
  }, [apiBase]);

  const filteredItems = useMemo(() => {
    const q = search.trim().toLowerCase();
    if (!q) return jsonItems;
    return jsonItems.filter((item) => {
      const id = String(item?.ID || "").toLowerCase();
      const title = getItemTitle(item).toLowerCase();
      return id.includes(q) || title.includes(q);
    });
  }, [jsonItems, search]);

  const selectedSet = useMemo(() => new Set(selectedIds), [selectedIds]);
  const selectedItems = useMemo(
    () => jsonItems.filter((it) => selectedSet.has(String(it.ID))),
    [jsonItems, selectedSet]
  );
  const firstSelectedItem = selectedItems[0] ?? null;

  const checkItems = useMemo(
    () => [
      { label: "API connected", ok: connectionOk },
      { label: "JSON files loaded", ok: jsonFiles.length > 0 },
      { label: "Items loaded", ok: jsonItems.length > 0 },
      { label: "Items selected", ok: selectedIds.length > 0 },
    ],
    [connectionOk, jsonFiles.length, jsonItems.length, selectedIds.length]
  );

  function setUiStatus(type, title, text) {
    setStatus({ type, title, text });
  }

  function withAccount(url) {
    const separator = url.includes("?") ? "&" : "?";
    return `${url}${separator}account=${encodeURIComponent(accountMode)}`;
  }

  function withSource(url) {
    const urlWithAccount = withAccount(url);
    if (!sourceFile) return urlWithAccount;
    const separator = urlWithAccount.includes("?") ? "&" : "?";
    return `${urlWithAccount}${separator}source_file=${encodeURIComponent(sourceFile)}`;
  }

  function stopUpdatePolling() {
    if (updatePollTimerRef.current) {
      clearInterval(updatePollTimerRef.current);
      updatePollTimerRef.current = null;
    }
  }

  async function pollUpdateJob(jobId) {
    const url = withAccount(`${endpoints.updateAsync}/${encodeURIComponent(jobId)}`);
    try {
      const res = await fetch(url);
      const text = await res.text();
      const data = parseResponseBody(text);
      const body = typeof data === "string" ? data : pretty(data);
      setRawOutput([`GET ${url}`, `HTTP ${res.status}`, "", body].join("\n"));
      setLastActionAt(Date.now());

      if (!res.ok) {
        setUiStatus("error", "Async update", `Status check failed: HTTP ${res.status}`);
        stopUpdatePolling();
        return;
      }

      const statusValue = String(data?.status || "");
      const progress = data?.progress || {};
      const processedItems = Number(progress?.processed_items || 0);
      const totalItems = Number(progress?.total_items || data?.result?.prepared || 0);
      const updatedItems = Number(progress?.updated || data?.result?.updated || 0);
      const failedItems = Number(progress?.failed || data?.result?.failed || 0);
      const phase = String(progress?.phase || "");

      if (statusValue === "queued") {
        setUiStatus("loading", "Async update", `Queued. Job: ${jobId}`);
        return;
      }
      if (statusValue === "running") {
        setUiStatus(
          "loading",
          "Async update",
          `Running: ${processedItems}/${totalItems} processed, updated ${updatedItems}, failed ${failedItems}${phase ? ` (${phase})` : ""}`
        );
        return;
      }
      if (statusValue === "completed") {
        setUiStatus(
          "success",
          "Async update",
          `Completed: updated ${updatedItems}, failed ${failedItems}, processed ${processedItems}/${totalItems}`
        );
        stopUpdatePolling();
        return;
      }
      if (statusValue === "failed") {
        setUiStatus("error", "Async update", `Failed: ${data?.error || "unknown error"}`);
        stopUpdatePolling();
      }
    } catch (e) {
      setUiStatus("error", "Async update", String(e));
      stopUpdatePolling();
    }
  }

  async function pollUploadJob(jobId) {
    const url = withAccount(`${endpoints.uploadAsync}/${encodeURIComponent(jobId)}`);
    try {
      const res = await fetch(url);
      const text = await res.text();
      const data = parseResponseBody(text);
      const body = typeof data === "string" ? data : pretty(data);
      setRawOutput([`GET ${url}`, `HTTP ${res.status}`, "", body].join("\n"));
      setLastActionAt(Date.now());

      if (!res.ok) {
        setUiStatus("error", "Async upload", `Status check failed: HTTP ${res.status}`);
        stopUpdatePolling();
        return;
      }

      const statusValue = String(data?.status || "");
      const progress = data?.progress || {};
      const processedItems = Number(progress?.processed_items || 0);
      const totalItems = Number(progress?.total_items || data?.result?.length || 0);
      const successItems = Number(progress?.success || 0);
      const failedItems = Number(progress?.failed || 0);
      const phase = String(progress?.phase || "");

      if (statusValue === "queued") {
        setUiStatus("loading", "Async upload", `Queued. Job: ${jobId}`);
        return;
      }
      if (statusValue === "running") {
        setUiStatus(
          "loading",
          "Async upload",
          `Running: ${processedItems}/${totalItems} processed, success ${successItems}, failed ${failedItems}${phase ? ` (${phase})` : ""}`
        );
        return;
      }
      if (statusValue === "completed") {
        setUiStatus(
          "success",
          "Async upload",
          `Completed: success ${successItems}, failed ${failedItems}, processed ${processedItems}/${totalItems}`
        );
        stopUpdatePolling();
        return;
      }
      if (statusValue === "failed") {
        setUiStatus("error", "Async upload", `Failed: ${data?.error || "unknown error"}`);
        stopUpdatePolling();
      }
    } catch (e) {
      setUiStatus("error", "Async upload", String(e));
      stopUpdatePolling();
    }
  }

  async function pollDeleteJob(jobId) {
    const url = withAccount(`${endpoints.deleteAsyncStatus}/${encodeURIComponent(jobId)}`);
    try {
      const res = await fetch(url);
      const text = await res.text();
      const data = parseResponseBody(text);
      const body = typeof data === "string" ? data : pretty(data);
      setRawOutput([`GET ${url}`, `HTTP ${res.status}`, "", body].join("\n"));
      setLastActionAt(Date.now());

      if (!res.ok) {
        setUiStatus("error", "Async delete", `Status check failed: HTTP ${res.status}`);
        stopUpdatePolling();
        return;
      }

      const statusValue = String(data?.status || "");
      const progress = data?.progress || {};
      const requested = Number(progress?.requested || 0);
      const deleted = Number(progress?.deleted || 0);
      const failed = Number(progress?.failed || 0);
      const phase = String(progress?.phase || "");

      if (statusValue === "queued") {
        setUiStatus("loading", "Async delete", `Queued. Job: ${jobId}`);
        return;
      }
      if (statusValue === "running") {
        setUiStatus(
          "loading",
          "Async delete",
          `Running: requested ${requested}, deleted ${deleted}, failed ${failed}${phase ? ` (${phase})` : ""}`
        );
        return;
      }
      if (statusValue === "completed") {
        setUiStatus("success", "Async delete", `Completed: deleted ${deleted}, failed ${failed}, requested ${requested}`);
        stopUpdatePolling();
        return;
      }
      if (statusValue === "failed") {
        setUiStatus("error", "Async delete", `Failed: ${data?.error || "unknown error"}`);
        stopUpdatePolling();
      }
    } catch (e) {
      setUiStatus("error", "Async delete", String(e));
      stopUpdatePolling();
    }
  }

  async function startDeleteAsync(url, label) {
    setLoading(true);
    setRawOutput("");
    setUiStatus("loading", label, "Starting async delete...");
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });
      const text = await res.text();
      const data = parseResponseBody(text);
      const body = typeof data === "string" ? data : pretty(data);
      setRawOutput([`POST ${url}`, `HTTP ${res.status}`, "", body].join("\n"));
      setLastActionAt(Date.now());

      if (!res.ok) {
        setUiStatus("error", label, `HTTP ${res.status}. Open technical details below.`);
        return;
      }
      const jobId = String(data?.job_id || "").trim();
      if (!jobId) {
        setUiStatus("error", label, "job_id is missing in response.");
        return;
      }
      setActiveUpdateJobId(jobId);
      stopUpdatePolling();
      setUiStatus("loading", label, `Queued. Job: ${jobId}`);
      await pollDeleteJob(jobId);
      updatePollTimerRef.current = setInterval(() => {
        pollDeleteJob(jobId);
      }, 2000);
    } catch (e) {
      setUiStatus("error", label, String(e));
    } finally {
      setLoading(false);
    }
  }

  async function call(method, url, label, payload = null) {
    setLoading(true);
    setRawOutput("");
    setUiStatus("loading", label, "Operation in progress...");
    try {
      const res = await fetch(url, {
        method,
        headers: { "Content-Type": "application/json" },
        body: payload ? JSON.stringify(payload) : null,
      });
      const text = await res.text();
      const data = parseResponseBody(text);
      const body = typeof data === "string" ? data : pretty(data);
      setRawOutput([`${method} ${url}`, `HTTP ${res.status}`, "", body].join("\n"));
      setLastActionAt(Date.now());

      if (res.ok) {
        setUiStatus("success", label, summarizeData(data, "Done"));
      } else {
        setUiStatus("error", label, `HTTP ${res.status}. Open technical details below.`);
      }
      return { ok: res.ok, status: res.status, data };
    } catch (e) {
      const msg = String(e);
      setRawOutput(msg);
      setLastActionAt(Date.now());
      setUiStatus("error", label, msg);
      return { ok: false, status: 0, data: null };
    } finally {
      setLoading(false);
    }
  }

  async function loadJsonFiles() {
    setLoading(true);
    setRawOutput("");
    setUiStatus("loading", "Load JSON files", "Fetching source files...");
    try {
      const filesUrl = withAccount(endpoints.jsonFiles);
      const res = await fetch(filesUrl);
      const text = await res.text();
      const data = parseResponseBody(text);
      const body = typeof data === "string" ? data : pretty(data);
      setRawOutput([`GET ${filesUrl}`, `HTTP ${res.status}`, "", body].join("\n"));
      setLastActionAt(Date.now());

      if (!res.ok) {
        setJsonFiles([]);
        setSourceFile("");
        setUiStatus("error", "Load JSON files", `HTTP ${res.status}`);
        return;
      }

      const files = Array.isArray(data?.files) ? data.files : [];
      setJsonFiles(files);

      if (files.length === 0) {
        setSourceFile("");
      } else {
        const stillExists = files.some((f) => f?.file_name === sourceFile);
        if (!stillExists) setSourceFile(files[0].file_name);
      }

      setUiStatus("success", "Load JSON files", `Files available: ${files.length}`);
    } catch (e) {
      const msg = String(e);
      setRawOutput(msg);
      setLastActionAt(Date.now());
      setUiStatus("error", "Load JSON files", msg);
    } finally {
      setLoading(false);
    }
  }

  async function checkConnection() {
    const result = await call("GET", withAccount(endpoints.docs), "Connection check");
    setConnectionOk(result.ok);
    if (result.ok) {
      await loadJsonFiles();
    }
  }

  async function loadJsonItems() {
    setLoading(true);
    setRawOutput("");
    setUiStatus("loading", "Load items", "Fetching items from JSON...");
    const url = withSource(`${endpoints.json}?limit=50`);
    try {
      const res = await fetch(url);
      const text = await res.text();
      const data = parseResponseBody(text);
      const body = typeof data === "string" ? data : pretty(data);
      setRawOutput([`GET ${url}`, `HTTP ${res.status}`, "", body].join("\n"));
      setLastActionAt(Date.now());

      if (!res.ok) {
        setJsonItems([]);
        setSelectedIds([]);
        setJsonTotal(null);
        setUiStatus("error", "Load items", `HTTP ${res.status}`);
        return;
      }

      const items = Array.isArray(data?.items) ? data.items : [];
      setJsonItems(items);
      setSelectedIds([]);
      setJsonTotal(typeof data?.total === "number" ? data.total : null);
      setUiStatus("success", "Load items", `Loaded ${items.length} items`);
    } catch (e) {
      const msg = String(e);
      setRawOutput(msg);
      setLastActionAt(Date.now());
      setUiStatus("error", "Load items", msg);
    } finally {
      setLoading(false);
    }
  }

  function toggleItemSelection(id) {
    setSelectedIds((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]));
  }

  async function validateSelected() {
    if (!selectedIds.length) return;
    const firstId = selectedIds[0];
    const url = withSource(`${endpoints.validateOne}${encodeURIComponent(firstId)}`);
    await call("POST", url, `Validate item #${firstId}`);
  }

  async function uploadSelected() {
    if (!selectedIds.length) return;
    if (!window.confirm(`Upload ${selectedIds.length} selected item(s)?`)) return;
    const selectedSnapshot = [...selectedIds];
    const result = await call(
      "POST",
      withSource(endpoints.uploadOneBulk),
      `Upload selected (${selectedSnapshot.length})`,
      { item_ids: selectedSnapshot }
    );
    if (!result.ok || !Array.isArray(result.data)) return;

    const successIds = result.data
      .filter((x) => x?.success === true)
      .map((x) => String(x?.item_id_local || "").trim())
      .filter(Boolean);

    if (!successIds.length) return;

    setJsonItems((prev) => prev.filter((item) => !successIds.includes(String(item.ID))));
    setSelectedIds((prev) => prev.filter((id) => !successIds.includes(id)));
  }

  async function updateSelected() {
    if (!selectedIds.length) return;
    if (!window.confirm(`Update ${selectedIds.length} selected item(s)?`)) return;
    const selectedSnapshot = [...selectedIds];
    await call(
      "POST",
      withSource(endpoints.updateOneBulk),
      `Update selected (${selectedSnapshot.length})`,
      { item_ids: selectedSnapshot }
    );
  }

  async function uploadAllFromSelectedFile() {
    if (!sourceFile) {
      setUiStatus("error", "Upload from selected file", "Select a source file first.");
      return;
    }
    if (!window.confirm(`Upload all items from selected file: ${sourceFile}?`)) return;
    const url = withSource(`${endpoints.uploadAsync}?limit=0`);
    setLoading(true);
    setRawOutput("");
    setUiStatus("loading", "Async upload", `Starting async upload for ${sourceFile}...`);
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });
      const text = await res.text();
      const data = parseResponseBody(text);
      const body = typeof data === "string" ? data : pretty(data);
      setRawOutput([`POST ${url}`, `HTTP ${res.status}`, "", body].join("\n"));
      setLastActionAt(Date.now());

      if (!res.ok) {
        setUiStatus("error", "Async upload", `HTTP ${res.status}. Open technical details below.`);
        return;
      }

      const jobId = String(data?.job_id || "").trim();
      if (!jobId) {
        setUiStatus("error", "Async upload", "job_id is missing in response.");
        return;
      }
      setActiveUpdateJobId(jobId);
      stopUpdatePolling();
      setUiStatus("loading", "Async upload", `Queued. Job: ${jobId}`);
      await pollUploadJob(jobId);
      updatePollTimerRef.current = setInterval(() => {
        pollUploadJob(jobId);
      }, 2000);
    } catch (e) {
      setUiStatus("error", "Async upload", String(e));
    } finally {
      setLoading(false);
    }
  }

  async function updateAllFromSelectedFile() {
    if (!sourceFile) {
      setUiStatus("error", "Update from selected file", "Select a source file first.");
      return;
    }
    if (!window.confirm(`Update all items from selected file: ${sourceFile}?`)) return;
    const url = withSource(`${endpoints.updateAsync}?limit=0`);
    setLoading(true);
    setRawOutput("");
    setUiStatus("loading", "Async update", `Starting async update for ${sourceFile}...`);
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
      });
      const text = await res.text();
      const data = parseResponseBody(text);
      const body = typeof data === "string" ? data : pretty(data);
      setRawOutput([`POST ${url}`, `HTTP ${res.status}`, "", body].join("\n"));
      setLastActionAt(Date.now());

      if (!res.ok) {
        setUiStatus("error", "Async update", `HTTP ${res.status}. Open technical details below.`);
        return;
      }

      const jobId = String(data?.job_id || "").trim();
      if (!jobId) {
        setUiStatus("error", "Async update", "job_id is missing in response.");
        return;
      }
      setActiveUpdateJobId(jobId);
      stopUpdatePolling();
      setUiStatus("loading", "Async update", `Queued. Job: ${jobId}`);
      await pollUpdateJob(jobId);
      updatePollTimerRef.current = setInterval(() => {
        pollUpdateJob(jobId);
      }, 2000);
    } catch (e) {
      setUiStatus("error", "Async update", String(e));
    } finally {
      setLoading(false);
    }
  }

  async function uploadAllFromFolder() {
    if (!window.confirm("Upload all items from JSON folder (all files)?")) return;
    const url = withAccount(`${endpoints.upload}?limit=0`);
    await call("POST", url, "Upload all from JSON folder");
  }

  async function loadFailedItems() {
    await call("GET", withAccount(endpoints.status), "Failed items list");
  }

  async function deleteByItemNumber() {
    const itemNumbers = deleteItemNumbers
      .split(/\r?\n/)
      .map((x) => x.trim())
      .filter(Boolean);
    if (!itemNumbers.length) {
      setUiStatus("error", "Delete items", "Provide at least one itemNumber.");
      return;
    }

    if (!window.confirm(`Delete ${itemNumbers.length} item(s) by itemNumber?`)) return;
    await call("POST", withAccount(endpoints.deleteByItemNumberBulk), `Delete (${itemNumbers.length})`, {
      item_numbers: itemNumbers,
    });
  }

  async function deleteAllInHood() {
    if (!window.confirm("Delete ALL items in Hood? This action cannot be undone.")) return;
    await startDeleteAsync(withAccount(endpoints.deleteAllAsync), "Async delete all");
  }

  async function deleteDuplicates() {
    if (!window.confirm("Delete duplicate EAN items and keep one item per EAN?")) return;
    await startDeleteAsync(
      withAccount(`${endpoints.deleteDuplicatesByEanAsync}?keep_one=true`),
      "Async delete duplicates by EAN"
    );
  }

  async function deleteAllFromSelectedFile() {
    if (!sourceFile) {
      setUiStatus("error", "Delete from selected file", "Select a source file first.");
      return;
    }
    if (!window.confirm(`Delete all items in Hood from selected file: ${sourceFile}?`)) return;
    await startDeleteAsync(withSource(endpoints.deleteBySourceFileAsync), `Async delete from file (${sourceFile})`);
  }

  useEffect(() => () => stopUpdatePolling(), []);

  const statusClass = `status status-${status.type}`;

  return (
    <div className="page">
      <header className="hero">
        <h1 className="title">Item Manager Panel</h1>
        <p className="subtitle">Check connection, choose source file, then upload products.</p>
      </header>

      <section className="card">
        <div className="topStrip">
          <div className={statusClass}>{loading ? "Working..." : status.title}</div>
          <div className="hint">
            Last action: <b>{formatTime(lastActionAt)}</b>
          </div>
        </div>
        <div className="checklist">
          {checkItems.map((step) => (
            <div key={step.label} className={`checkItem ${step.ok ? "ok" : ""}`}>
              <span>{step.ok ? "OK" : "..."}</span>
              <p>{step.label}</p>
            </div>
          ))}
        </div>
      </section>

      <section className="card">
        <div className="cardTop">
          <div>
            <h2 className="cardTitle">1) Connection and Source File</h2>
            <p className="cardHint">Load JSON file list and select one source for operations.</p>
          </div>
        </div>

        <details className="advanced">
          <summary>Connection settings</summary>
          <div className="row">
            <label className="label">
              Account
              <select
                className="input"
                value={accountMode}
                onChange={(e) => {
                  setAccountMode(e.target.value);
                  setJsonFiles([]);
                  setSourceFile("");
                  setJsonItems([]);
                  setSelectedIds([]);
                }}
                disabled={loading}
              >
                <option value="jvmoebel">jvmoebel</option>
                <option value="xlmoebel">xlmoebel</option>
              </select>
            </label>
            <label className="label grow">
              API URL
              <input
                className="input"
                value={apiBase}
                onChange={(e) => setApiBase(e.target.value)}
                placeholder="http://localhost:8000"
              />
            </label>
          </div>
        </details>

        <div className="row">
          <button className="btn" disabled={loading} onClick={checkConnection}>
            Check connection
          </button>
          <button className="btn" disabled={loading} onClick={loadJsonFiles}>
            Refresh JSON files
          </button>
          <label className="label grow">
            Source file
            <select
              className="input"
              value={sourceFile}
              onChange={(e) => setSourceFile(e.target.value)}
              disabled={loading || jsonFiles.length === 0}
            >
              {jsonFiles.length === 0 ? <option value="">No files</option> : null}
              {jsonFiles.map((file) => (
                <option key={file.file_name} value={file.file_name}>
                  {file.file_name} ({file.item_count})
                </option>
              ))}
            </select>
          </label>
          <button className="btn primary" disabled={loading} onClick={loadJsonItems}>
            Load items
          </button>
        </div>

        <div className="metrics">
          <div className="metric">
            <span>Items in table</span>
            <b>{jsonItems.length}</b>
          </div>
          <div className="metric">
            <span>Total in source</span>
            <b>{jsonTotal ?? "-"}</b>
          </div>
          <div className="metric">
            <span>Selected file</span>
            <b>{sourceFile || "-"}</b>
          </div>
        </div>
      </section>

      <section className="card">
        <h2 className="cardTitle">2) Item Selection</h2>
        <div className="row">
          <label className="label grow">
            Search by ID or title
            <input
              className="input"
              value={search}
              onChange={(e) => setSearch(e.target.value)}
              placeholder="Example: 83859 or sofa"
              disabled={loading}
            />
          </label>
        </div>

        <div className="itemsWrap">
          {filteredItems.length === 0 ? (
            <div className="empty">{jsonItems.length === 0 ? "No items loaded yet." : "No matches."}</div>
          ) : (
            <table className="itemsTable">
              <thead>
                <tr>
                  <th>Select</th>
                  <th>ID</th>
                  <th>Title</th>
                </tr>
              </thead>
              <tbody>
                {filteredItems.slice(0, 50).map((item) => {
                  const id = String(item.ID);
                  const active = selectedSet.has(id);
                  return (
                    <tr key={id} className={active ? "rowActive" : ""} onClick={() => toggleItemSelection(id)}>
                      <td>
                        <input
                          type="checkbox"
                          checked={active}
                          onClick={(e) => e.stopPropagation()}
                          onChange={() => toggleItemSelection(id)}
                        />
                      </td>
                      <td>{id}</td>
                      <td>{getItemTitle(item)}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
        </div>

        <div className="selectedCard">
          <div>
            <span>Selected count</span>
            <b>{selectedIds.length || 0}</b>
          </div>
          <div>
            <span>itemNumber</span>
            <b>{firstSelectedItem ? getItemNumber(firstSelectedItem) : "-"}</b>
          </div>
          <div className="selectedWide">
            <span>First selected title</span>
            <b>{firstSelectedItem ? getItemTitle(firstSelectedItem) : "-"}</b>
          </div>
        </div>
      </section>

      <section className="card">
        <h2 className="cardTitle">3) Actions</h2>
        <p className="cardHint">Upload selected items, selected source file, or all JSON files.</p>

        <div className="row">
          <button className="btn" disabled={loading || !selectedIds.length} onClick={validateSelected}>
            Validate first selected
          </button>
          <button className="btn primary" disabled={loading || !selectedIds.length} onClick={uploadSelected}>
            Upload selected ({selectedIds.length})
          </button>
          <button className="btn" disabled={loading || !selectedIds.length} onClick={updateSelected}>
            Update selected ({selectedIds.length})
          </button>
          <button className="btn warning" disabled={loading || !sourceFile} onClick={uploadAllFromSelectedFile}>
            Upload all from selected file
          </button>
          <button className="btn warning" disabled={loading || !sourceFile} onClick={updateAllFromSelectedFile}>
            Update all from selected file
          </button>
          <button className="btn warning" disabled={loading} onClick={uploadAllFromFolder}>
            Upload all JSON files
          </button>
          <button className="btn" disabled={loading} onClick={loadFailedItems}>
            Show failed items
          </button>
        </div>
      </section>

      <section className="card dangerZone">
        <h2 className="cardTitle">Delete by itemNumber</h2>
        <p className="cardHint">One itemNumber per line.</p>
        <div className="row">
          <label className="label grow">
            itemNumber list
            <textarea
              className="input"
              value={deleteItemNumbers}
              onChange={(e) => setDeleteItemNumbers(e.target.value)}
              placeholder={"4069943027235\n4069943027174\n4069943027001"}
              disabled={loading}
            />
          </label>
          <button className="btn danger" disabled={loading || !deleteItemNumbers.trim()} onClick={deleteByItemNumber}>
            Delete in Hood
          </button>
          <button className="btn danger" disabled={loading || !sourceFile} onClick={deleteAllFromSelectedFile}>
            Delete all from selected file
          </button>
          <button className="btn danger" disabled={loading} onClick={deleteDuplicates}>
            Delete duplicates
          </button>
          <button className="btn danger" disabled={loading} onClick={deleteAllInHood}>
            Delete ALL in Hood
          </button>
        </div>
      </section>

      <section className="card">
        <h2 className="cardTitle">Result</h2>
        <div className="resultMain">{status.text}</div>
        {activeUpdateJobId ? <div className="hint">Active async job: <code>{activeUpdateJobId}</code></div> : null}
        <details className="advanced">
          <summary>Technical details</summary>
          <pre className="output">{rawOutput || "-"}</pre>
        </details>
        <div className="hint">
          Logs: <code>backend/logs/items.log</code>
        </div>
      </section>
    </div>
  );
}
