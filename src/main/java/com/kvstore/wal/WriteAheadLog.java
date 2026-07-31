package com.kvstore.wal;

import jakarta.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Write-Ahead Log (WAL)
 *
 * WHY a WAL?
 *   If a node crashes mid-write, in-memory data is lost. A WAL writes every
 *   mutation to disk BEFORE acknowledging it. On restart, we replay the log
 *   to reconstruct state — giving us crash-recovery / durability.
 *
 * FORMAT: one operation per line
 *   PUT|<key>|<value>|<epochMs>
 *   DELETE|<key>|<epochMs>
 */
@Component
public class WriteAheadLog {

    private static final Logger log = Logger.getLogger(WriteAheadLog.class.getName());

    @Value("${wal.file.path:/tmp/wal.log}")
    private String walFilePath;

    private BufferedWriter writer;

    @PostConstruct
    public void init() throws IOException {
        Path path = Paths.get(walFilePath);
        // Append mode: we never truncate so old entries survive restarts
        writer = Files.newBufferedWriter(path,
                StandardOpenOption.CREATE,
                StandardOpenOption.APPEND);
        log.info("WAL initialised at: " + walFilePath);
    }

    /** Persist a PUT before it is applied to in-memory storage. */
    public synchronized void logPut(String key, String value) {
        writeLine("PUT|" + key + "|" + value + "|" + Instant.now().toEpochMilli());
    }

    /** Persist a DELETE before it is applied to in-memory storage. */
    public synchronized void logDelete(String key) {
        writeLine("DELETE|" + key + "|" + Instant.now().toEpochMilli());
    }

    /**
     * Replay the WAL to rebuild in-memory state after a crash.
     * Returns a map representing the final state (later entries win).
     */
    public Map<String, String> replay() throws IOException {
        Map<String, String> state = new LinkedHashMap<>();
        Path path = Paths.get(walFilePath);
        if (!Files.exists(path)) return state;

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            int replayed = 0;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|", -1);
                if (parts.length < 2) continue;
                switch (parts[0]) {
                    case "PUT"    -> { if (parts.length >= 3) state.put(parts[1], parts[2]); }
                    case "DELETE" -> state.remove(parts[1]);
                }
                replayed++;
            }
            log.info("WAL replay complete — " + replayed + " entries, "
                     + state.size() + " keys reconstructed");
        }
        return state;
    }

    private void writeLine(String line) {
        try {
            writer.write(line);
            writer.newLine();
            writer.flush(); // flush every write — critical for durability
        } catch (IOException e) {
            throw new RuntimeException("WAL write failed", e);
        }
    }
}
