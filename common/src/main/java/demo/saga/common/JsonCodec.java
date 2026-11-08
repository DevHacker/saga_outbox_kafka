package demo.saga.common;

import tools.jackson.databind.json.JsonMapper;

/** Jackson 3 (Spring Boot 4) : les exceptions sont non vérifiées. */
public final class JsonCodec {
    private static final JsonMapper MAPPER = JsonMapper.builder().build();

    public static String write(Object value) {
        return MAPPER.writeValueAsString(value);
    }

    public static <T> T read(String json, Class<T> type) {
        return MAPPER.readValue(json, type);
    }

    private JsonCodec() {
    }
}
