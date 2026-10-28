package dev.transerver.core;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

/** Cancel oversized HTTP responses before the client allocates an unbounded byte array. */
final class LimitedBodySubscriber implements HttpResponse.BodySubscriber<byte[]> {
    private final HttpResponse.BodySubscriber<byte[]> delegate = HttpResponse.BodySubscribers.ofByteArray();
    private final long maximum;
    private long received;
    private Flow.Subscription subscription;
    private boolean failed;
    LimitedBodySubscriber(long maximum) { this.maximum = maximum; }
    public CompletionStage<byte[]> getBody() { return delegate.getBody(); }
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        delegate.onSubscribe(subscription);
    }
    public void onNext(List<ByteBuffer> buffers) {
        if (failed) return;
        for (var buffer : buffers) received += buffer.remaining();
        if (received > maximum) {
            failed = true;
            subscription.cancel();
            delegate.onError(new IOException("HTTP response exceeds " + maximum + " bytes"));
        } else delegate.onNext(buffers);
    }
    public void onError(Throwable error) { if (!failed) delegate.onError(error); }
    public void onComplete() { if (!failed) delegate.onComplete(); }
}
