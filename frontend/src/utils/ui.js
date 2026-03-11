export function pretty(obj) {
  try {
    return JSON.stringify(obj, null, 2);
  } catch {
    return String(obj);
  }
}

export function parseResponseBody(text) {
  try {
    return JSON.parse(text);
  } catch {
    return text;
  }
}

export function getItemTitle(item) {
  return String(item?.Artikelbeschreibung || item?.Name || item?.title || "Item");
}

export function getItemNumber(item) {
  return String(item?.itemNumber || item?.item_number || item?.Artikelnummer || item?.article_number || "-");
}

export function formatTime(ts) {
  if (!ts) return "-";
  return new Date(ts).toLocaleString();
}

export function summarizeData(data, label) {
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

export function buildEndpoints(apiBase) {
  const base = apiBase.replace(/\/+$/, "");
  return {
    docs: `${base}/docs`,
    upload: `${base}/items/upload`,
    uploadAsync: `${base}/items/upload_async`,
    uploadManyAsync: `${base}/items/upload_many_async`,
    update: `${base}/items/update`,
    updateAsync: `${base}/items/update_async`,
    updateAll: `${base}/items/update_all`,
    updateAllAsync: `${base}/items/update_all_async`,
    deleteAsyncStatus: `${base}/items/delete_async`,
    status: `${base}/items/status`,
    uploadedSplit: `${base}/items/uploaded_split`,
    uploadedSplitAsync: `${base}/items/uploaded_split_async`,
    json: `${base}/items/json`,
    jsonFiles: `${base}/items/json/files`,
    checkSelectedFiles: `${base}/items/check_selected_files`,
    validateOne: `${base}/items/validate_one/`,
    uploadOneBulk: `${base}/items/upload_one`,
    updateOneBulk: `${base}/items/update_one`,
    deleteByItemNumberBulk: `${base}/items/delete/by-item-number`,
    deleteBySourceFile: `${base}/items/delete/by-source-file`,
    deleteBySourceFileAsync: `${base}/items/delete/by-source-file_async`,
    deleteBySourceFilesAsync: `${base}/items/delete/by-source-files_async`,
    deleteAll: `${base}/items/delete/all`,
    deleteAllAsync: `${base}/items/delete/all_async`,
  };
}
