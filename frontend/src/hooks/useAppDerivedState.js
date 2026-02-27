import { useMemo } from "react";

import { getItemTitle } from "../utils/ui";

export function useAppDerivedState({
  jsonItems,
  search,
  selectedIds,
  sourceFilesMulti,
  connectionOk,
  jsonFiles,
}) {
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
  const selectedFilesSet = useMemo(() => new Set(sourceFilesMulti), [sourceFilesMulti]);

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

  return {
    filteredItems,
    selectedSet,
    selectedFilesSet,
    selectedItems,
    firstSelectedItem,
    checkItems,
  };
}
