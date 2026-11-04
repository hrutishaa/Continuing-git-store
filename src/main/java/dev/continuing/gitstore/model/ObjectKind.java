package dev.continuing.gitstore.model;

public enum ObjectKind {
    BLOB("blob"),
    TREE("tree"),
    COMMIT("commit");

    private final String wire;

    ObjectKind(String wire) {
        this.wire = wire;
    }

    public String wire() {
        return wire;
    }

    public static ObjectKind fromWire(String wire) {
        for (ObjectKind kind : values()) {
            if (kind.wire.equals(wire)) {
                return kind;
            }
        }
        throw new IllegalArgumentException("unknown object kind " + wire);
    }
}
