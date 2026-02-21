package io.conduit.core;

import io.conduit.config.Props;

/**
 * Factory for one component type, discovered with {@link java.util.ServiceLoader}. Onboarding a new
 * source, transform or sink is one class implementing the matching provider, one line in its
 * {@code META-INF/services} file, and one YAML entry. The core never changes.
 *
 * <p>{@link #create} reads its fields from {@code props}; any field it does not read is reported to
 * the user as unknown.
 */
public interface Provider<T> {
    /** The {@code type:} value in YAML, e.g. {@code postgres}. */
    String type();

    T create(Props props, ComponentContext context);
}
