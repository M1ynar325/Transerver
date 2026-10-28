package dev.transerver.core;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.regex.Pattern;

public final class FileMessageStore implements MessageStore {
    private static final Pattern SAFE_NAME = Pattern.compile("[A-Za-z0-9._-]{1,160}");
    private final Path root;
    // Queue files are owned by this process. Load filenames once; never retain payloads here.
    private final java.util.Map<String, java.util.NavigableSet<String>> indexes = new java.util.HashMap<>();

    private java.util.NavigableSet<String> index(String area) throws IOException {
        var cached = indexes.get(area);
        if (cached != null) return cached;
        var keys = new java.util.TreeSet<String>();
        Path directory = areaPath(area);
        if (Files.isDirectory(directory)) {
            try (var files = Files.list(directory)) {
                files.map(path -> path.getFileName().toString()).filter(name -> name.endsWith(".bin"))
                        .forEach(name -> keys.add(name.substring(0, name.length() - 4)));
            }
        }
        indexes.put(area, keys);
        return keys;
    }

    public FileMessageStore(Path root) throws IOException {
        this.root = root.toAbsolutePath().normalize();
        Files.createDirectories(this.root);
    }

    @Override
    public synchronized void put(String area, String key, byte[] value) throws IOException {
        Path target = entryPath(area, key);
        var keys = index(area);
        Files.createDirectories(target.getParent());
        if (Files.exists(target)) {
            byte[] current = Files.readAllBytes(target);
            if (!Arrays.equals(current, value)) {
                throw new IOException("Entry already exists with different content: " + area + "/" + key);
            }
            return;
        }
        Path temporary = target.resolveSibling("." + key + "." + UUID.randomUUID() + ".tmp");
        try (FileChannel channel = FileChannel.open(temporary,
                StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE)) {
            ByteBuffer buffer = ByteBuffer.wrap(value);
            while (buffer.hasRemaining()) {
                channel.write(buffer);
            }
            channel.force(true);
        }
        try {
            Files.move(temporary, target, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException exception) {
            Files.move(temporary, target);
        } finally {
            Files.deleteIfExists(temporary);
        }
        keys.add(key);
    }

    @Override
    public synchronized Optional<byte[]> get(String area, String key) throws IOException {
        Path target = entryPath(area, key);
        return Files.exists(target) ? Optional.of(Files.readAllBytes(target)) : Optional.empty();
    }

    @Override
    public synchronized List<StoredEntry> list(String area, int limit) throws IOException {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must not be negative");
        }
        return listAfter(area, null, limit, Integer.MAX_VALUE);
    }

    @Override
    public synchronized List<StoredEntry> listAfter(String area, String after, int limit, int maxBytes)
            throws IOException {
        if (limit < 0 || maxBytes < 0) throw new IllegalArgumentException("negative batch limit");
        var entries = new ArrayList<StoredEntry>();
        long bytes = 0;
        var keys = after == null ? index(area) : index(area).tailSet(after, false);
        for (String key : keys) {
            if (entries.size() >= limit) break;
            Path path = entryPath(area, key);
            long size = Files.size(path);
            if (bytes + size > maxBytes) break;
            entries.add(new StoredEntry(key, Files.readAllBytes(path)));
            bytes += size;
        }
        return List.copyOf(entries);
    }

    @Override
    public synchronized int count(String area) throws IOException {
        return index(area).size();
    }

    @Override
    public synchronized void remove(String area, String key) throws IOException {
        Files.deleteIfExists(entryPath(area, key));
        index(area).remove(key);
    }

    private Path entryPath(String area, String key) {
        requireSafe(key, "key");
        return areaPath(area).resolve(key + ".bin");
    }

    private Path areaPath(String area) {
        requireSafe(area, "area");
        return root.resolve(area);
    }

    private static void requireSafe(String value, String label) {
        if (value == null || !SAFE_NAME.matcher(value).matches()) {
            throw new IllegalArgumentException("Unsafe " + label + ": " + value);
        }
    }
}
