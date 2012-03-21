package test;

import java.io.Serializable;
import java.util.UUID;

public class TestMessage implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final UUID uuid;
    private final int duration;

    public TestMessage(UUID uuid, int duration) {
        this.uuid = uuid;
        this.duration = duration;
    }

    public UUID getUuid() {
        return uuid;
    }

    public int getDuration() {
        return duration;
    }
}
