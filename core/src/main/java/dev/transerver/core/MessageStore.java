package dev.transerver.core;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

public interface MessageStore {
    void put(String area, String key, byte[] value) throws IOException;

    Optional<byte[]> get(String area, String key) throws IOException;

    List<StoredEntry> list(String area, int limit) throws IOException;

    /**
     * Counts entries without requiring callers to materialize their payloads. Stores that can do
     * this cheaply should override it; the default preserves compatibility for other backends.
     */
    default int count(String area) throws IOException {
        return list(area, Integer.MAX_VALUE).size();
    }

    default List<StoredEntry> listAfter(String area, String after, int limit, int maxBytes) throws IOException {
        var result = new java.util.ArrayList<StoredEntry>();
        int bytes = 0;
        for (var entry : list(area, Integer.MAX_VALUE)) {
            if (after != null && entry.key().compareTo(after) <= 0) continue;
            if (result.size() >= limit || bytes + entry.value().length > maxBytes) break;
            result.add(entry);
            bytes += entry.value().length;
        }
        return List.copyOf(result);
    }

    void remove(String area, String key) throws IOException;

    record StoredEntry(String key, byte[] value) {
    }
}
