package io.conduit.core;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Random v4 UUIDs without {@link UUID#randomUUID()}, which draws from a shared SecureRandom and
 * becomes a contention point at hundreds of thousands of events per second. Event ids need to be
 * unique, not unpredictable.
 */
public final class Ids {
    private Ids() {
    }

    public static String next() {
        ThreadLocalRandom r = ThreadLocalRandom.current();
        long msb = (r.nextLong() & ~0xF000L) | 0x4000L;
        long lsb = (r.nextLong() & 0x3FFFFFFFFFFFFFFFL) | 0x8000000000000000L;
        return new UUID(msb, lsb).toString();
    }
}
