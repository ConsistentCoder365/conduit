package io.conduit.sink;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.io.SerializedString;
import io.conduit.config.Props;
import io.conduit.core.ComponentContext;
import io.conduit.core.Event;
import io.conduit.core.Json;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * Rolling JSON-lines files. With several lanes, each lane writes its own files (the lane number is
 * part of the name), so lanes never share a file handle.
 *
 * <pre>
 * - name: archive
 *   type: file
 *   dir: out/archive
 *   maxFileBytes: 128MB
 *   rollEvery: 1h     # optional, roll by age as well as size
 *   fsync: false      # true = force to disk on every batch (slower, survives power loss)
 * </pre>
 */
public final class FileSink implements Sink {

    public static final class Provider implements SinkProvider {
        @Override
        public String type() {
            return "file";
        }

        @Override
        public Sink create(Props p, ComponentContext ctx) {
            String prefix = p.string("prefix", ctx.name());
            if (ctx.lanes() > 1) {
                prefix = prefix + "-" + ctx.lane();
            }
            return new FileSink(new RollingFileWriter(Path.of(p.string("dir")), prefix,
                    p.bytes("maxFileBytes", 128L << 20), p.duration("rollEvery", null), p.bool("fsync", false)));
        }
    }

    private final RollingFileWriter writer;
    private final ByteArrayOutputStream buffer = new ByteArrayOutputStream(256 * 1024);

    public FileSink(RollingFileWriter writer) {
        this.writer = writer;
    }

    @Override
    public void open() throws IOException {
        writer.open();
    }

    @Override
    public void write(List<Event> batch) throws IOException {
        buffer.reset();
        try (JsonGenerator g = Json.FACTORY.createGenerator(buffer)) {
            g.setRootValueSeparator(new SerializedString("\n"));
            for (Event e : batch) {
                Json.writeEvent(g, e);
            }
            g.writeRaw('\n');
        }
        writer.write(buffer.toByteArray(), 0, buffer.size());
        writer.flush();
    }

    @Override
    public void flush() throws IOException {
        writer.flush();
    }

    @Override
    public void close() throws IOException {
        writer.close();
    }

    Path currentFile() {
        return writer.current();
    }
}
