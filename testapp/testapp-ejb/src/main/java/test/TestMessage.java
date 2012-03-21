package test;

import java.io.Serializable;
import java.util.UUID;

public class TestMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final UUID uuid;
    private final int duration;
    private final boolean fail;

    public TestMessage(UUID uuid, int duration, boolean fail) {
        this.uuid = uuid;
        this.duration = duration;
        this.fail = fail;
    }

    public UUID getUuid() {
        return uuid;
    }

    public int getDuration() {
        return duration;
    }

    public boolean isFail() {
        return fail;
    }
}
