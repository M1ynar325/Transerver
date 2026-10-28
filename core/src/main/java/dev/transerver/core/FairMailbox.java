package dev.transerver.core;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;

/** Bounded fresh-message preference plus a durable round-robin cursor. No payloads are cached. */
final class FairMailbox {
    static final int MAX_BYTES = 4 * 1024 * 1024;
    private final ArrayBlockingQueue<String> fresh = new ArrayBlockingQueue<>(256);
    private String cursor;

    void added(String key) { fresh.offer(key); } // Losing a hint never loses a durable message.

    synchronized List<MessageStore.StoredEntry> next(MessageStore store, String area, int limit) throws IOException {
        if (limit <= 0) return List.of();
        var result = new ArrayList<MessageStore.StoredEntry>();
        var selected = new HashSet<String>();
        int bytes = 0;
        // Reserve room for at least one maximum-sized historical message.
        int freshBudget = MAX_BYTES - EnvelopeCodec.MAX_PAYLOAD_BYTES - 16 * 1024;
        for (int i = 0; i < Math.max(1, limit / 2); i++) {
            String key = fresh.poll();
            if (key == null) break;
            var value = store.get(area, key);
            if (value.isEmpty() || bytes + value.get().length > freshBudget || !selected.add(key)) continue;
            result.add(new MessageStore.StoredEntry(key, value.get()));
            bytes += value.get().length;
        }
        var page = store.listAfter(area, cursor, limit - result.size(), MAX_BYTES - bytes);
        if (page.isEmpty() && cursor != null) {
            cursor = null;
            page = store.listAfter(area, null, limit - result.size(), MAX_BYTES - bytes);
        }
        for (var entry : page) {
            cursor = entry.key();
            if (selected.add(entry.key())) result.add(entry);
        }
        return List.copyOf(result);
    }
}
