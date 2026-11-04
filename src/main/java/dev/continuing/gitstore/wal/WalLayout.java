package dev.continuing.gitstore.wal;

import java.util.Locale;

/** Object keys for one repository. The WAL file name is the total order. */
public final class WalLayout {
    private WalLayout() {}

    public static String object(String repo, String hash) {
        return repo + "/objects/" + hash;
    }

    public static String wal(String repo, long seq) {
        return repo + "/wal/" + String.format(Locale.ROOT, "%020d", seq) + ".json";
    }

    public static String index(String repo, long seq) {
        return repo + "/index/" + String.format(Locale.ROOT, "%020d", seq) + ".json";
    }

    public static String tip(String repo) {
        return repo + "/tip";
    }
}
