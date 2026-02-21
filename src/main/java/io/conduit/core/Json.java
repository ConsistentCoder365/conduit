package io.conduit.core;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.time.format.DateTimeFormatter;
import java.util.Map;

/** Shared, thread-safe JSON plumbing and the one canonical way an {@link Event} is written. */
public final class Json {
    public static final ObjectMapper MAPPER = JsonMapper.builder()
            .disable(SerializationFeature.FLUSH_AFTER_WRITE_VALUE)
            .build();
    public static final JsonFactory FACTORY = MAPPER.getFactory();
    public static final TypeReference<Map<String, Object>> MAP = new TypeReference<>() {
    };

    private Json() {
    }

    public static void writeEvent(JsonGenerator g, Event e) throws IOException {
        g.writeStartObject();
        g.writeStringField("id", e.id());
        g.writeStringField("timestamp", DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(e.timestamp()));
        g.writeStringField("source", e.source());
        g.writeStringField("type", e.type());
        g.writeStringField("severity", e.severity().name());
        g.writeStringField("key", e.key());
        g.writeFieldName("attributes");
        MAPPER.writeValue(g, e.attributes());
        g.writeEndObject();
    }

    public static String toJson(Event e) {
        StringWriter out = new StringWriter(256);
        try (JsonGenerator g = FACTORY.createGenerator(out)) {
            writeEvent(g, e);
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
        return out.toString();
    }

    public static String attributesJson(Event e) {
        try {
            return MAPPER.writeValueAsString(e.attributes());
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }
}
