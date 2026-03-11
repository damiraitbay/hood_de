import React, { useEffect, useMemo, useRef, useState } from "react";

import { useAppDerivedState } from "./hooks/useAppDerivedState";
import {
  buildEndpoints,
  formatTime,
  getItemNumber,
  getItemTitle,
  parseResponseBody,
  pretty,
  summarizeData,
} from "./utils/ui";

export default function App() {
  const [apiBase, setApiBase] = useState("/api");
  const [accountMode, setAccountMode] = useState("jvmoebel");
  const [jsonFiles, setJsonFiles] = useState([]);
  const [sourceFile, setSourceFile] = useState("");
  const [sourceFilesMulti, setSourceFilesMulti] = useState([]);
  const [jsonItems, setJsonItems] = useState([]);
  const [selectedIds, setSelectedIds] = useState([]);
  const [jsonTotal, setJsonTotal] = useState(null);
  const [search, setSearch] = useState("");
  const [deleteItemNumbers, setDeleteItemNumbers] = useState("");
  const [loading, setLoading] = useState(false);
  const [rawOutput, setRawOutput] = useState("");
  const [processLogs, setProcessLogs] = useState([]);
  const [lastActionAt, setLastActionAt] = useState(null);
  const [activeUpdateJobId, setActiveUpdateJobId] = useState("");
  const [connectionOk, setConnectionOk] = useState(false);
  const [uploadedSplitLists, setUploadedSplitLists] = useState({ uploaded: [], notUploaded: [] });
  const updatePollTimerRef = useRef(null);
  const [status, setStatus] = useState({
    type: "idle",
    title: "Ready",
    text: "Check API connection first.",
  });

  const endpoints = useMemo(() => buildEndpoints(apiBase), [apiBase]);
  const { filteredItems, selectedSet, selectedFilesSet, firstSelectedItem, checkItems } = useAppDerivedState({
    jsonItems,
    search,
    selectedIds,
    sourceFilesMulti,
    connectionOk,
    jsonFiles,
  });

  function setUiStatus(type, title, text) {
    setStatus({ type, title, text });
  }

  function pushProcessLog(processName, message) {
    const ts = new Date().toLocaleTimeString();
    const line = `[${ts}] [${processName}] ${message}`;
    setProcessLogs((prev) => {
      const next = [...prev, line];
      return next.slice(-300);
    });
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

  function toSplitListItem(rawItem) {
    if (!rawItem || typeof rawItem !== "object") {
      return {
        id: "-",
        itemNumber: String(rawItem || "-"),
        title: "Item",
        factory: "-",
      };
    }
    const id = String(rawItem.ID ?? rawItem.id ?? "").trim() || "-";
    const itemNumber =
      String(
        rawItem.checked_item_number ??
        rawItem.item_number ??
        rawItem.ItemNumber ??
        rawItem.EAN ??
        rawItem.ean ??
        ""
      ).trim() || "-";
    const title = getItemTitle(rawItem) || "Item";
    const factory = String(rawItem.factory ?? rawItem.__source_name__ ?? "").trim() || "-";
    return { id, itemNumber, title, factory };
  }

  function buildSplitListsFromResult(result) {
    const uploadedRaw = Array.isArray(result?.uploaded) ? result.uploaded : [];
    const notUploadedRaw = Array.isArray(result?.not_uploaded) ? result.not_uploaded : [];
    return {
      uploaded: uploadedRaw.map(toSplitListItem),
      notUploaded: notUploadedRaw.map(toSplitListItem),
    };
  }

  function renderSplitList(items, label) {
    if (!Array.isArray(items) || items.length === 0) {
      return `${label}: no items`;
    }
    const lines = items.map(
      (item, idx) =>
        `${idx + 1}. ${item.title} | factory: ${item.factory} | itemNumber: ${item.itemNumber} | id: ${item.id}`
    );
    return `${label}: ${items.length}\n\n${lines.join("\n")}`;
  }

  async function pollUpdateJob(jobId, statusBase = endpoints.updateAsync, label = "Async update", processName = "update_async") {
    const url = withAccount(`${statusBase}/${encodeURIComponent(jobId)}`);
    try {
      const res = await fetch(url);
      const text = await res.text();
      const data = parseResponseBody(text);
      const body = typeof data === "string" ? data : pretty(data);
      setRawOutput([`GET ${url}`, `HTTP ${res.status}`, "", body].join("\n"));
      setLastActionAt(Date.now());

      if (!res.ok) {
        setUiStatus("error", label, `Status check failed: HTTP ${res.status}`);
        stopUpdatePolling();
        return;
      }

      const statusValue = String(data?.status || "");
      const progress = data?.progress || {};
      const processedItems = Number(progress?.processed_items || 0);
      const totalItems = Number(progress?.total_items || progress?.prepared || data?.result?.prepared || 0);
      const updatedItems = Number(progress?.updated || data?.result?.updated || 0);
      const failedItems = Number(progress?.failed || data?.result?.failed || 0);
      const phase = String(progress?.phase || "");
      pushProcessLog(processName, `status=${statusValue || "-"} phase=${phase || "-"}`);

      if (statusValue === "queued") {
        setUiStatus("loading", label, `Queued. Job: ${jobId}`);
        return;
      }
      if (statusValue === "running") {
        setUiStatus(
          "loading",
          label,
          `Running: ${processedItems}/${totalItems} processed, updated ${updatedItems}, failed ${failedItems}${phase ? ` (${phase})` : ""}`
        );
        return;
      }
      if (statusValue === "completed") {
        setUiStatus(
          "success",
          label,
          `Completed: updated ${updatedItems}, failed ${failedItems}, processed ${processedItems}/${totalItems}`
        );
        stopUpdatePolling();
        return;
      }
      if (statusValue === "failed") {
        setUiStatus("error", label, `Failed: ${data?.error || "unknown error"}`);
        stopUpdatePolling();
      }
    } catch (e) {
      setUiStatus("error", label, String(e));
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
      const filesTotal = Number(progress?.files_total || 0);
      const filesCompleted = Number(progress?.files_completed || 0);
      const isManyFiles = filesTotal > 0 || data?.mode === "many_files";
      const fileProcessedItems = Number(progress?.file_processed_items || 0);
      const fileTotalItems = Number(progress?.file_total_items || 0);
      const processedItems = Number(progress?.processed_items || 0);
      const totalItems = Number(progress?.total_items || data?.result?.length || 0);
      const successItems = Number(progress?.success || 0);
      const failedItems = Number(progress?.failed || 0);
      const phase = String(progress?.phase || "");
      pushProcessLog("upload_async", `status=${statusValue || "-"} phase=${phase || "-"}`);

      if (statusValue === "queued") {
        setUiStatus("loading", "Async upload", `Queued. Job: ${jobId}`);
        return;
      }
      if (statusValue === "running") {
        const manyFilesText =
          fileTotalItems > 0
            ? `Running: files ${filesCompleted}/${filesTotal}, current file ${fileProcessedItems}/${fileTotalItems}, success ${successItems}, failed ${failedItems}`
            : `Running: files ${filesCompleted}/${filesTotal}, success ${successItems}, failed ${failedItems}`;
        const mainText = isManyFiles
          ? manyFilesText
          : `Running: ${processedItems}/${totalItems} processed, success ${successItems}, failed ${failedItems}`;
        setUiStatus("loading", "Async upload", `${mainText}${phase ? ` (${phase})` : ""}`);
        return;
      }
      if (statusValue === "completed") {
        const completedText = isManyFiles
          ? `Completed: files ${filesCompleted}/${filesTotal}, success ${successItems}, failed ${failedItems}`
          : `Completed: success ${successItems}, failed ${failedItems}, processed ${processedItems}/${totalItems}`;
        setUiStatus("success", "Async upload", completedText);
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
      const requestedRaw = progress?.requested ?? data?.result?.requested ?? null;
      const deletedRaw = progress?.deleted ?? data?.result?.deleted ?? null;
      const failedRaw = progress?.failed ?? data?.result?.failed ?? null;
      const requested = requestedRaw == null ? null : Number(requestedRaw);
      const deleted = deletedRaw == null ? null : Number(deletedRaw);
      const failed = failedRaw == null ? null : Number(failedRaw);
      const filesTotal = Number(progress?.files_total ?? data?.result?.files_total ?? 0);
      const filesCompleted = Number(progress?.files_completed ?? data?.result?.files_completed ?? 0);
      const failedFiles = Number(progress?.failed_files ?? data?.result?.failed_files ?? 0);
      const phase = String(progress?.phase || "");
      pushProcessLog("delete_async", `status=${statusValue || "-"} phase=${phase || "-"}`);

      if (statusValue === "queued") {
        setUiStatus("loading", "Async delete", `Queued. Job: ${jobId}`);
        return;
      }
      if (statusValue === "running") {
        if (phase === "taking_ids") {
          const processedIds = Number(progress?.processed ?? 0);
          const totalIds = Number(progress?.total ?? 0);
          const collectedIds = Number(progress?.collected ?? 0);
          const suffix = totalIds > 0 ? `${processedIds}/${totalIds}` : `${processedIds}`;
          setUiStatus(
            "loading",
            "Async delete",
            `Taking IDs: ${suffix}, collected ${collectedIds}`
          );
          return;
        }
        if (phase === "loading_items") {
          const fetchedItems = Number(progress?.fetched_items ?? 0);
          const totalItems = Number(progress?.total_items ?? 0);
          const suffix = totalItems > 0 ? `${fetchedItems}/${totalItems}` : `${fetchedItems}`;
          setUiStatus("loading", "Async delete", `Loading items: ${suffix}`);
          return;
        }
        if (phase === "deleting_files") {
          setUiStatus(
            "loading",
            "Async delete",
            `Running: files ${filesCompleted}/${filesTotal}, requested ${requested ?? "-"}, deleted ${deleted ?? "-"}, failed ${failed ?? "-"}, failed files ${failedFiles}`
          );
          return;
        }
        const metrics =
          requested == null && deleted == null && failed == null
            ? "Running: processing..."
            : `Running: requested ${requested ?? "-"}, deleted ${deleted ?? "-"}, failed ${failed ?? "-"}`;
        setUiStatus("loading", "Async delete", `${metrics}${phase ? ` (${phase})` : ""}`);
        return;
      }
      if (statusValue === "completed") {
        setUiStatus(
          "success",
          "Async delete",
          filesTotal > 0
            ? `Completed: files ${filesCompleted}/${filesTotal}, deleted ${deleted ?? 0}, failed ${failed ?? 0}, failed files ${failedFiles}, requested ${requested ?? 0}`
            : `Completed: deleted ${deleted ?? 0}, failed ${failed ?? 0}, requested ${requested ?? 0}`
        );
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

  async function pollUploadedSplitJob(jobId) {
    const url = withAccount(`${endpoints.uploadedSplitAsync}/${encodeURIComponent(jobId)}`);
    try {
      const res = await fetch(url);
      const text = await res.text();
      const data = parseResponseBody(text);
      const body = typeof data === "string" ? data : pretty(data);
      setRawOutput([`GET ${url}`, `HTTP ${res.status}`, "", body].join("\n"));
      setLastActionAt(Date.now());

      if (!res.ok) {
        setUiStatus("error", "Uploaded split", `Status check failed: HTTP ${res.status}`);
        stopUpdatePolling();
        return;
      }

      const statusValue = String(data?.status || "");
      const progress = data?.progress || {};
      const phase = String(progress?.phase || "");
      pushProcessLog("uploaded_split_async", `status=${statusValue || "-"} phase=${phase || "-"}`);
      const statusesDone = Number(progress?.statuses_done || 0);
      const statusesTotal = Number(progress?.statuses_total || 0);
      const processedItems = Number(progress?.processed_items || 0);
      const totalItems = Number(progress?.total_items || 0);
      const uploaded = Number(progress?.uploaded || data?.result?.uploaded_count || 0);
      const notUploaded = Number(progress?.not_uploaded || data?.result?.not_uploaded_count || 0);

      if (statusValue === "queued") {
        setUiStatus("loading", "Uploaded split", `Queued. Job: ${jobId}`);
        return;
      }
      if (statusValue === "running") {
        if (phase === "loading_hood_items") {
          setUiStatus(
            "loading",
            "Uploaded split",
            `Loading Hood items: statuses ${statusesDone}/${statusesTotal}`
          );
          return;
        }
        if (phase === "splitting_local_items") {
          setUiStatus(
            "loading",
            "Uploaded split",
            `Splitting local items: ${processedItems}/${totalItems}, uploaded ${uploaded}, not uploaded ${notUploaded}`
          );
          return;
        }
        setUiStatus("loading", "Uploaded split", `Running...${phase ? ` (${phase})` : ""}`);
        return;
      }
      if (statusValue === "completed") {
        const partial = Boolean(data?.result?.partial);
        const warningsCount = Array.isArray(data?.result?.warnings) ? data.result.warnings.length : 0;
        setUploadedSplitLists(buildSplitListsFromResult(data?.result || {}));
        setUiStatus(
          "success",
          "Uploaded split",
          `Completed: uploaded ${uploaded}, not uploaded ${notUploaded}${partial ? `, partial with warnings ${warningsCount}` : ""}`
        );
        stopUpdatePolling();
        return;
      }
      if (statusValue === "failed") {
        setUiStatus("error", "Uploaded split", `Failed: ${data?.error || "unknown error"}`);
        stopUpdatePolling();
      }
    } catch (e) {
      setUiStatus("error", "Uploaded split", String(e));
      stopUpdatePolling();
    }
  }

  async function startDeleteAsync(url, label, payload = null) {
    setLoading(true);
    setRawOutput("");
    setProcessLogs([]);
    pushProcessLog("delete_async", "starting job");
    setUiStatus("loading", label, "Starting async delete...");
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: payload ? JSON.stringify(payload) : null,
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
      pushProcessLog("delete_async", `queued job_id=${jobId}`);
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
    setProcessLogs([]);
    pushProcessLog("request", `start ${method} ${url}`);
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
        pushProcessLog("request", `done ${method} ${url} -> HTTP ${res.status}`);
        setUiStatus("success", label, summarizeData(data, "Done"));
      } else {
        pushProcessLog("request", `failed ${method} ${url} -> HTTP ${res.status}`);
        setUiStatus("error", label, `HTTP ${res.status}. Open technical details below.`);
      }
      return { ok: res.ok, status: res.status, data };
    } catch (e) {
      const msg = String(e);
      pushProcessLog("request", `error ${method} ${url}: ${msg}`);
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
        setSourceFilesMulti([]);
      } else {
        const stillExists = files.some((f) => f?.file_name === sourceFile);
        if (!stillExists) setSourceFile(files[0].file_name);
        setSourceFilesMulti((prev) => {
          const available = new Set(files.map((f) => String(f.file_name)));
          const next = prev.filter((x) => available.has(x));
          if (next.length > 0) return next;
          return [String(files[0].file_name)];
        });
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

  function toggleSourceFileSelection(fileName) {
    setSourceFilesMulti((prev) => (prev.includes(fileName) ? prev.filter((x) => x !== fileName) : [...prev, fileName]));
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
    setProcessLogs([]);
    pushProcessLog("upload_async", `starting from source_file=${sourceFile}`);
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
      pushProcessLog("upload_async", `queued job_id=${jobId}`);
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

  async function uploadSelectedFiles() {
    if (!sourceFilesMulti.length) {
      setUiStatus("error", "Upload selected files", "Select one or more source files first.");
      return;
    }
    if (!window.confirm(`Upload all items from ${sourceFilesMulti.length} selected file(s)?`)) return;
    const url = withAccount(`${endpoints.uploadManyAsync}?limit=0`);
    setLoading(true);
    setRawOutput("");
    setProcessLogs([]);
    pushProcessLog("upload_many_async", `starting with files=${sourceFilesMulti.length}`);
    setUiStatus("loading", "Async upload", `Starting async upload for ${sourceFilesMulti.length} file(s)...`);
    try {
      const res = await fetch(url, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ source_files: sourceFilesMulti }),
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
      pushProcessLog("upload_many_async", `queued job_id=${jobId}`);
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
    const url = withSource(`${endpoints.updateAsync}?limit=0&workers=3`);
    setLoading(true);
    setRawOutput("");
    setProcessLogs([]);
    pushProcessLog("update_async", `starting from source_file=${sourceFile}`);
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
      pushProcessLog("update_async", `queued job_id=${jobId}`);
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

  async function updateAllFromFolder() {
    if (!window.confirm("Update all items from JSON folder (all files)?")) return;
    const url = withAccount(`${endpoints.updateAllAsync}?limit=0&workers=3`);
    setLoading(true);
    setRawOutput("");
    setProcessLogs([]);
    pushProcessLog("update_all_async", "starting");
    setUiStatus("loading", "Async update (all JSON files)", "Starting async update for all JSON files...");
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
        setUiStatus("error", "Async update (all JSON files)", `HTTP ${res.status}. Open technical details below.`);
        return;
      }

      const jobId = String(data?.job_id || "").trim();
      if (!jobId) {
        setUiStatus("error", "Async update (all JSON files)", "job_id is missing in response.");
        return;
      }
      setActiveUpdateJobId(jobId);
      stopUpdatePolling();
      pushProcessLog("update_all_async", `queued job_id=${jobId}`);
      setUiStatus("loading", "Async update (all JSON files)", `Queued. Job: ${jobId}`);
      await pollUpdateJob(jobId, endpoints.updateAllAsync, "Async update (all JSON files)", "update_all_async");
      updatePollTimerRef.current = setInterval(() => {
        pollUpdateJob(jobId, endpoints.updateAllAsync, "Async update (all JSON files)", "update_all_async");
      }, 2000);
    } catch (e) {
      setUiStatus("error", "Async update (all JSON files)", String(e));
    } finally {
      setLoading(false);
    }
  }

  async function checkSelectedFilesInHood() {
    if (!sourceFilesMulti.length) {
      setUiStatus("error", "Check selected files", "Select one or more source files first.");
      return;
    }
    await call(
      "POST",
      withAccount(endpoints.checkSelectedFiles),
      `Check selected files (${sourceFilesMulti.length})`,
      { source_files: sourceFilesMulti }
    );
  }

  async function loadFailedItems() {
    await call("GET", withAccount(endpoints.status), "Failed items list");
  }

  async function loadUploadedSplit() {
    const url = withAccount(endpoints.uploadedSplitAsync);
    setLoading(true);
    setRawOutput("");
    setProcessLogs([]);
    setUploadedSplitLists({ uploaded: [], notUploaded: [] });
    pushProcessLog("uploaded_split_async", "starting job");
    setUiStatus("loading", "Uploaded split", "Starting async split...");
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
        setUiStatus("error", "Uploaded split", `HTTP ${res.status}. Open technical details below.`);
        return;
      }

      const jobId = String(data?.job_id || "").trim();
      if (!jobId) {
        setUiStatus("error", "Uploaded split", "job_id is missing in response.");
        return;
      }
      setActiveUpdateJobId(jobId);
      stopUpdatePolling();
      pushProcessLog("uploaded_split_async", `queued job_id=${jobId}`);
      setUiStatus("loading", "Uploaded split", `Queued. Job: ${jobId}`);
      await pollUploadedSplitJob(jobId);
      updatePollTimerRef.current = setInterval(() => {
        pollUploadedSplitJob(jobId);
      }, 2000);
    } catch (e) {
      setUiStatus("error", "Uploaded split", String(e));
    } finally {
      setLoading(false);
    }
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

  function showUploadedItemsList() {
    const items = uploadedSplitLists.uploaded || [];
    setRawOutput(renderSplitList(items, "Uploaded items"));
    setLastActionAt(Date.now());
    setUiStatus("success", "Uploaded items", `Items: ${items.length}`);
  }

  function showNotUploadedItemsList() {
    const items = uploadedSplitLists.notUploaded || [];
    setRawOutput(renderSplitList(items, "Not uploaded items"));
    setLastActionAt(Date.now());
    setUiStatus("success", "Not uploaded items", `Items: ${items.length}`);
  }

  async function deleteAllInHood() {
    if (!window.confirm("Delete ALL items in Hood? This action cannot be undone.")) return;
    await startDeleteAsync(withAccount(endpoints.deleteAllAsync), "Async delete all");
  }

  async function deleteAllFromSelectedFile() {
    if (!sourceFile) {
      setUiStatus("error", "Delete from selected file", "Select a source file first.");
      return;
    }
    if (!window.confirm(`Delete all items in Hood from selected file: ${sourceFile}?`)) return;
    await startDeleteAsync(withSource(endpoints.deleteBySourceFileAsync), `Async delete from file (${sourceFile})`);
  }

  async function deleteAllFromSelectedFiles() {
    if (!sourceFilesMulti.length) {
      setUiStatus("error", "Delete from selected files", "Select one or more source files first.");
      return;
    }
    if (!window.confirm(`Delete all items in Hood from ${sourceFilesMulti.length} selected file(s)?`)) return;
    await startDeleteAsync(
      withAccount(endpoints.deleteBySourceFilesAsync),
      `Async delete from selected files (${sourceFilesMulti.length})`,
      { source_files: sourceFilesMulti }
    );
  }

  useEffect(() => () => stopUpdatePolling(), []);

  const statusClass = `status status-${status.type}`;

  return (
    <div className="siteShell">
      <header className="siteHeader">
        <div className="siteBrand">
          <h1 className="title">Item Manager Panel</h1>
          <p className="subtitle">Check connection, choose source file, then upload products.</p>
        </div>
        <div className="siteHeaderMeta">
          <div className={statusClass}>{loading ? "Working..." : status.title}</div>
          <div className="hint">
            Last action: <b>{formatTime(lastActionAt)}</b>
          </div>
        </div>
      </header>

      <div className="siteBody">
        <aside className="siteSidebar">
          <div className="sideCard">
            <h3>Navigation</h3>
            <nav className="sideNav">
              <a href="#overview">Overview</a>
              <a href="#connection">Connection</a>
              <a href="#selection">Item Selection</a>
              <a href="#actions">Actions</a>
              <a href="#delete">Delete</a>
              <a href="#result">Result</a>
            </nav>
          </div>
          <div className="sideCard">
            <h3>Quick Actions</h3>
            <div className="sideActions">
              <button className="btn" disabled={loading} onClick={checkConnection}>
                Check connection
              </button>
              <button className="btn" disabled={loading} onClick={loadJsonFiles}>
                Refresh JSON files
              </button>
              <button className="btn primary" disabled={loading} onClick={loadJsonItems}>
                Load items
              </button>
            </div>
          </div>
        </aside>

        <main className="siteMain">
          <section id="overview" className="card">
            <div className="topStrip">
              <h2 className="cardTitle">Overview</h2>
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

          <section id="connection" className="card">
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
                  setSourceFilesMulti([]);
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

        <div className="itemsWrap">
          {jsonFiles.length === 0 ? (
            <div className="empty">No source files loaded yet.</div>
          ) : (
            <table className="itemsTable">
              <thead>
                <tr>
                  <th>Select</th>
                  <th>File</th>
                  <th>Items</th>
                </tr>
              </thead>
              <tbody>
                {jsonFiles.map((file) => {
                  const name = String(file.file_name);
                  const active = selectedFilesSet.has(name);
                  return (
                    <tr key={`f-${name}`} className={active ? "rowActive" : ""} onClick={() => toggleSourceFileSelection(name)}>
                      <td>
                        <input
                          type="checkbox"
                          checked={active}
                          onClick={(e) => e.stopPropagation()}
                          onChange={() => toggleSourceFileSelection(name)}
                        />
                      </td>
                      <td>{name}</td>
                      <td>{file.item_count}</td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          )}
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

          <section id="selection" className="card">
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

          <section id="actions" className="card">
            <h2 className="cardTitle">3) Actions</h2>
            <p className="cardHint">Upload/update selected items, selected source file, or all JSON files.</p>

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
              <button className="btn warning" disabled={loading || !sourceFilesMulti.length} onClick={uploadSelectedFiles}>
                Upload selected files ({sourceFilesMulti.length})
              </button>
              <button className="btn" disabled={loading || !sourceFilesMulti.length} onClick={checkSelectedFilesInHood}>
                Check selected files ({sourceFilesMulti.length})
              </button>
              <button className="btn warning" disabled={loading || !sourceFile} onClick={updateAllFromSelectedFile}>
                Update all from selected file
              </button>
              <button className="btn warning" disabled={loading} onClick={uploadAllFromFolder}>
                Upload all JSON files
              </button>
              <button className="btn warning" disabled={loading} onClick={updateAllFromFolder}>
                Update all JSON files
              </button>
              <button className="btn" disabled={loading} onClick={loadFailedItems}>
                Show failed items
              </button>
              <button className="btn" disabled={loading} onClick={loadUploadedSplit}>
                Show uploaded/not uploaded
              </button>
              <button className="btn" disabled={loading || !(uploadedSplitLists.uploaded || []).length} onClick={showUploadedItemsList}>
                List of uploaded items ({(uploadedSplitLists.uploaded || []).length})
              </button>
              <button className="btn" disabled={loading || !(uploadedSplitLists.notUploaded || []).length} onClick={showNotUploadedItemsList}>
                List of not uploaded items ({(uploadedSplitLists.notUploaded || []).length})
              </button>
            </div>
          </section>

          <section id="delete" className="card dangerZone">
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
              <button className="btn danger" disabled={loading || !sourceFilesMulti.length} onClick={deleteAllFromSelectedFiles}>
                Delete all from selected files ({sourceFilesMulti.length})
              </button>
              <button className="btn danger" disabled={loading} onClick={deleteAllInHood}>
                Delete ALL in Hood
              </button>
            </div>
          </section>

          <section id="result" className="card">
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
        </main>
      </div>

      <footer className="siteFooter">
        <span>Hood Manager Panel</span>
        <span>Account: {accountMode}</span>
        <span>API: {apiBase}</span>
      </footer>
    </div>
  );
}
