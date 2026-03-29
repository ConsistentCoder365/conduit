package io.conduit.source;

import io.conduit.config.Props;
import io.conduit.core.ComponentContext;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.Objects;

/**
 * Tails a JSON-lines file like {@code tail -F}: follows appends, starts over when the file is
 * truncated, and reopens it when it is rotated (replaced by a new file at the same path). Because the
 * emit blocks when the pipeline is full, a backed-up pipeline simply stops this source reading.
 *
 * <pre>
 * - name: auth-log
 *   type: file
 *   path: /var/log/auth.jsonl
 *   fromBeginning: true   # false = only lines appended after startup
 *   exitAtEof: false      # true = read the file once and finish (batch import, tests)
 *   pollInterval: 200ms
 * </pre>
 */
public final class FileTailSource implements Source {

    public static final class Provider implements SourceProvider {
        @Override
        public String type() {
            return "file";
        }

        @Override
        public Source create(Props p, ComponentContext ctx) {
            return new FileTailSource(Path.of(p.string("path")), p.bool("fromBeginning", true),
                    p.bool("exitAtEof", false), p.duration("pollInterval", Duration.ofMillis(200)),
                    (int) p.bytes("maxLineBytes", 1 << 20));
        }
    }

    private final Path path;
    private final boolean fromBeginning;
    private final boolean exitAtEof;
    private final long pollMillis;
    private final int maxLineBytes;
    private volatile boolean running = true;

    FileTailSource(Path path, boolean fromBeginning, boolean exitAtEof, Duration pollInterval, int maxLineBytes) {
        this.path = path;
        this.fromBeginning = fromBeginning;
        this.exitAtEof = exitAtEof;
        this.pollMillis = Math.max(1, pollInterval.toMillis());
        this.maxLineBytes = maxLineBytes;
    }

    @Override
    public void open() throws IOException {
        if (exitAtEof && !Files.isReadable(path)) {
            throw new NoSuchFileException(path.toString(), null, "file is missing or unreadable");
        }
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        ByteBuffer buf = ByteBuffer.allocate(64 * 1024);
        LineAssembler lines = new LineAssembler(ctx, maxLineBytes);
        boolean firstOpen = true;
        FileChannel ch = null;
        Object fileKey = null;
        try {
            while (running) {
                if (ch == null) {
                    if (!Files.exists(path)) {
                        if (exitAtEof) {
                            return;
                        }
                        Thread.sleep(pollMillis);
                        continue;
                    }
                    ch = FileChannel.open(path, StandardOpenOption.READ);
                    fileKey = fileKey(path);
                    if (firstOpen && !fromBeginning) {
                        ch.position(ch.size());
                    }
                    firstOpen = false;
                }
                buf.clear();
                int n = ch.read(buf);
                if (n > 0) {
                    buf.flip();
                    lines.accept(buf);
                    continue;
                }
                if (exitAtEof) {
                    lines.finish();
                    return;
                }
                Object currentKey = fileKey(path);
                if (currentKey != null && !Objects.equals(currentKey, fileKey)) {
                    // Rotated: the old file is fully read (we are at its EOF), so switch to the new one.
                    lines.finish();
                    ch.close();
                    ch = null;
                    continue;
                }
                if (currentKey != null && Files.size(path) < ch.position()) {
                    lines.reset();
                    ch.position(0);
                    continue;
                }
                Thread.sleep(pollMillis);
            }
        } finally {
            if (ch != null) {
                ch.close();
            }
        }
    }

    private static Object fileKey(Path p) {
        try {
            BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class);
            Object k = attrs.fileKey();
            return k != null ? k : attrs.creationTime();
        } catch (IOException e) {
            return null;
        }
    }

    @Override
    public void stop() {
        running = false;
    }

    /** Splits a byte stream on newlines, carrying partial lines across reads. */
    static final class LineAssembler {
        private final SourceContext ctx;
        private final int maxLineBytes;
        private final ByteArrayOutputStream partial = new ByteArrayOutputStream(1024);
        private boolean discarding;

        LineAssembler(SourceContext ctx, int maxLineBytes) {
            this.ctx = ctx;
            this.maxLineBytes = maxLineBytes;
        }

        void accept(ByteBuffer buf) throws InterruptedException {
            byte[] arr = buf.array();
            int start = buf.position();
            int end = buf.limit();
            for (int i = start; i < end; i++) {
                if (arr[i] == '\n') {
                    append(arr, start, i - start);
                    emitLine();
                    start = i + 1;
                }
            }
            append(arr, start, end - start);
        }

        private void append(byte[] arr, int off, int len) {
            if (discarding || len == 0) {
                return;
            }
            if (partial.size() + len > maxLineBytes) {
                ctx.deadLetter(new String(partial.toByteArray(), 0, Math.min(partial.size(), 256),
                        StandardCharsets.UTF_8) + "…", "line longer than " + maxLineBytes + " bytes");
                partial.reset();
                discarding = true;
                return;
            }
            partial.write(arr, off, len);
        }

        private void emitLine() throws InterruptedException {
            if (discarding) {
                discarding = false;
                return;
            }
            String line = partial.toString(StandardCharsets.UTF_8);
            partial.reset();
            if (line.endsWith("\r")) {
                line = line.substring(0, line.length() - 1);
            }
            if (!line.isBlank()) {
                ctx.emitJson(line);
            }
        }

        /** End of input: a trailing line without a newline still counts. */
        void finish() throws InterruptedException {
            if (partial.size() > 0 || discarding) {
                emitLine();
            }
        }

        void reset() {
            partial.reset();
            discarding = false;
        }
    }
}
