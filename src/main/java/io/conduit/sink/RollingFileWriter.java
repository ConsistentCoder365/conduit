package io.conduit.sink;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Appends bytes to {@code <prefix>-<timestamp>-<seq>.jsonl} and starts a new file when the current
 * one would pass {@code maxBytes} or is older than {@code rollEvery}. Writes go straight to the file
 * channel (callers hand over whole batches), so once {@link #write} returns the data is in the OS.
 * With {@code fsync} on, {@link #flush} also forces it to the disk. Not thread-safe.
 */
public final class RollingFileWriter implements Closeable {
    private static final DateTimeFormatter STAMP = DateTimeFormatter.ofPattern("yyyyMMdd-HHmmss");

    private final Path dir;
    private final String prefix;
    private final long maxBytes;
    private final long rollEveryNanos;
    private final boolean fsync;
    private FileChannel channel;
    private Path current;
    private long written;
    private long openedAt;
    private int seq;

    public RollingFileWriter(Path dir, String prefix, long maxBytes, Duration rollEvery, boolean fsync) {
        this.dir = dir;
        this.prefix = prefix;
        this.maxBytes = maxBytes;
        this.rollEveryNanos = rollEvery == null ? Long.MAX_VALUE : rollEvery.toNanos();
        this.fsync = fsync;
    }

    public void open() throws IOException {
        Files.createDirectories(dir);
        if (!Files.isWritable(dir)) {
            throw new IOException("directory is not writable: " + dir);
        }
    }

    public void write(byte[] bytes, int offset, int length) throws IOException {
        if (channel == null || (written > 0 && written + length > maxBytes)
                || System.nanoTime() - openedAt > rollEveryNanos) {
            roll();
        }
        ByteBuffer buf = ByteBuffer.wrap(bytes, offset, length);
        while (buf.hasRemaining()) {
            channel.write(buf);
        }
        written += length;
    }

    public void flush() throws IOException {
        if (channel != null && fsync) {
            channel.force(false);
        }
    }

    public Path current() {
        return current;
    }

    private void roll() throws IOException {
        closeChannel();
        Files.createDirectories(dir);
        String name = String.format("%s-%s-%04d.jsonl", prefix, LocalDateTime.now().format(STAMP), seq++);
        current = dir.resolve(name);
        channel = FileChannel.open(current, StandardOpenOption.CREATE, StandardOpenOption.WRITE,
                StandardOpenOption.APPEND);
        written = channel.size();
        openedAt = System.nanoTime();
    }

    private void closeChannel() throws IOException {
        if (channel != null) {
            if (fsync) {
                channel.force(true);
            }
            channel.close();
            channel = null;
        }
    }

    @Override
    public void close() throws IOException {
        closeChannel();
    }
}
