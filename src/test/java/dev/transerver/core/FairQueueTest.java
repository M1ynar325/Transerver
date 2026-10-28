package dev.transerver.core;

import dev.transerver.api.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

class FairQueueTest {
    @TempDir Path root;
    private MessageEnvelope message(int id, String destination, int size) {
        return new MessageEnvelope(new UUID(0, id), "test:data", "source", destination, "", Instant.now(),
                null, "application/octet-stream", new byte[size]);
    }
    @Test void offlinePrefixCannotStarveLiveDestinationAfterRestart() throws Exception {
        var store = new FileMessageStore(root.resolve("source"));
        var codec = new EnvelopeCodec();
        for (int i = 1; i <= 256; i++) {
            var m = message(i, "offline", 10);
            store.put("outbox", m.messageId().toString(), codec.encode(m));
        }
        var live = message(257, "live", 10);
        store.put("outbox", live.messageId().toString(), codec.encode(live));
        var router = new FileRouterTransport(new FileMessageStore(root.resolve("router")), Set.of("source", "offline", "live"));
        var source = new TranserverNode("source", router, new FileMessageStore(root.resolve("source")));
        var receiver = new TranserverNode("live", router, new FileMessageStore(root.resolve("live")));
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        receiver.registerHandler("test:data", m -> { calls.incrementAndGet(); return CompletableFuture.completedFuture(DeliveryResult.APPLIED); });
        for (int i = 0; i < 25; i++) { source.pump(); receiver.pump(); }
        assertEquals(1, calls.get());
        assertEquals(256, source.pendingOutboxCount());
        assertEquals(1, source.completedSends(10).size());
    }
    @Test void routerRotatesPastUnacknowledgedPrefixAndBoundsBytes() throws Exception {
        var store = new FileMessageStore(root);
        var codec = new EnvelopeCodec();
        String area = "relay-" + Base64.getUrlEncoder().withoutPadding().encodeToString("live".getBytes());
        for (int i = 1; i <= 140; i++) {
            var m = message(i, "live", 64 * 1024);
            store.put(area, m.messageId().toString(), codec.encode(m));
        }
        var router = new FileRouterTransport(new FileMessageStore(root), Set.of("source", "live"));
        var seen = new HashSet<UUID>();
        for (int i = 0; i < 8; i++) {
            var batch = router.receive("live", 128);
            assertTrue(batch.size() <= 32);
            int bytes = 0;
            for (var m : batch) { bytes += codec.encode(m).length; seen.add(m.messageId()); }
            assertTrue(bytes <= FairMailbox.MAX_BYTES);
        }
        assertEquals(140, seen.size());
        assertEquals(140, store.count(area)); // RETRY preserves custody.
    }
    @Test void freshTrafficHasPriorityWithoutStarvingHistory() throws Exception {
        var store = new FileMessageStore(root);
        for (int i = 0; i < 300; i++) store.put("queue", String.format("%05d", i), new byte[4]);
        var queue = new FairMailbox();
        var seen = new HashSet<String>();
        for (int pass = 0; pass < 40; pass++) {
            for (int i = 0; i < 8; i++) {
                String key = "z" + pass + "-" + i;
                store.put("queue", key, new byte[4]); queue.added(key);
            }
            var batch = queue.next(store, "queue", 16);
            assertTrue(batch.stream().anyMatch(e -> e.key().startsWith("z")));
            batch.forEach(e -> seen.add(e.key()));
        }
        for (int i = 0; i < 300; i++) assertTrue(seen.contains(String.format("%05d", i)));
    }
    @Test void handlerCompletionDoesNotWriteOnCallingThread() throws Exception {
        var router = new InMemoryRouterTransport(); router.registerNode("source"); router.registerNode("live");
        var source = new TranserverNode("source", router, new FileMessageStore(root.resolve("source")));
        var store = new FileMessageStore(root.resolve("live"));
        var live = new TranserverNode("live", router, store);
        var future = new CompletableFuture<DeliveryResult>();
        live.registerHandler("test:data", m -> future);
        source.send("live", "test:data", new byte[0], SendOptions.defaults());
        source.pump(); live.pump();
        future.complete(DeliveryResult.APPLIED);
        assertEquals(0, store.count("completed"));
        live.pump();
        assertEquals(1, store.count("completed"));
        source.pump(); assertEquals(0, source.pendingOutboxCount());
    }
    @Test void maximumPayloadProgressesWithinBatchBudget() throws Exception {
        var router = new FileRouterTransport(new FileMessageStore(root), Set.of("source", "live"));
        for (int i = 1; i <= 3; i++) router.relay(message(i, "live", EnvelopeCodec.MAX_PAYLOAD_BYTES));
        var seen = new HashSet<UUID>();
        var codec = new WireBatchCodec();
        for (int i = 0; i < 4; i++) {
            var batch = router.receive("live", 128);
            byte[] encoded = codec.encodeMessages(batch);
            assertTrue(encoded.length <= WireBatchCodec.MAX_BATCH_BYTES);
            codec.decodeMessages(encoded).forEach(m -> seen.add(m.messageId()));
        }
        assertEquals(3, seen.size());
        assertThrows(java.io.IOException.class, () -> codec.decodeMessages(new byte[WireBatchCodec.MAX_BATCH_BYTES + 1]));
    }

    @Test void pendingChannelDoesNotBlockOtherHandlers() throws Exception {
        var router = new FileRouterTransport(new FileMessageStore(root.resolve("router")), Set.of("source", "live"));
        var store = new FileMessageStore(root.resolve("live"));
        var codec = new EnvelopeCodec();
        for (int i = 1; i <= 64; i++) {
            var m = message(i, "live", 0);
            store.put("inbox", m.messageId().toString(), codec.encode(m));
        }
        var hello = new MessageEnvelope(new UUID(0, 65), "test:hello", "source", "live", "", Instant.now(), null, "text/plain", new byte[0]);
        store.put("inbox", hello.messageId().toString(), codec.encode(hello));
        var live = new TranserverNode("live", router, store);
        var pending = new java.util.concurrent.atomic.AtomicInteger();
        var received = new java.util.concurrent.atomic.AtomicInteger();
        live.registerHandler("test:data", m -> { pending.incrementAndGet(); return new CompletableFuture<>(); });
        live.registerHandler("test:hello", m -> { received.incrementAndGet(); return CompletableFuture.completedFuture(DeliveryResult.APPLIED); });
        for (int i = 0; i < 4; i++) live.pump();
        assertEquals(32, pending.get());
        assertEquals(1, received.get());
    }

    @Test void concurrentSendersAndPumpsKeepAllReceipts() throws Exception {
        var router = new FileRouterTransport(new FileMessageStore(root.resolve("router")), Set.of("source", "live"));
        var source = new TranserverNode("source", router, new FileMessageStore(root.resolve("source")));
        var live = new TranserverNode("live", router, new FileMessageStore(root.resolve("live")));
        var calls = new java.util.concurrent.atomic.AtomicInteger();
        live.registerHandler("test:data", m -> { calls.incrementAndGet(); return CompletableFuture.completedFuture(DeliveryResult.APPLIED); });
        var handles = new CopyOnWriteArrayList<SendHandle>();
        try (var pool = Executors.newFixedThreadPool(4)) {
            var sends = new ArrayList<Future<?>>();
            for (int worker = 0; worker < 4; worker++) sends.add(pool.submit(() -> {
                for (int i = 0; i < 32; i++) {
                    handles.add(source.send("live", "test:data", new byte[32], SendOptions.defaults()));
                    source.pump(); live.pump();
                }
            }));
            for (var send : sends) send.get(30, TimeUnit.SECONDS);
        }
        for (int i = 0; i < 30 && source.pendingOutboxCount() > 0; i++) { source.pump(); live.pump(); }
        assertEquals(128, calls.get());
        assertEquals(0, source.pendingOutboxCount());
        for (var handle : handles) assertEquals(DeliveryState.APPLIED,
                handle.completion().toCompletableFuture().get(2, TimeUnit.SECONDS).state());
    }

    @Test void bodySubscriberCancelsOversizedResponse() {
        var subscriber = new LimitedBodySubscriber(4);
        var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        subscriber.onSubscribe(new Flow.Subscription() {
            public void request(long n) {} public void cancel() { cancelled.set(true); }
        });
        subscriber.onNext(List.of(java.nio.ByteBuffer.wrap(new byte[5])));
        assertTrue(cancelled.get());
        assertTrue(subscriber.getBody().toCompletableFuture().isCompletedExceptionally());
    }
}
