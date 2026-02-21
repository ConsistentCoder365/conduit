package io.conduit.config;

/** Invalid or unusable configuration. Raised before any pipeline thread starts. */
public class ConfigException extends RuntimeException {
    public ConfigException(String message) {
        super(message);
    }

    public ConfigException(String message, Throwable cause) {
        super(message, cause);
    }
}
