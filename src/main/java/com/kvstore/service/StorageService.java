package com.kvstore.service;

import com.kvstore.wal.WriteAheadLog;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

/**
 * StorageService
 *
 * The local in-memory store backed by a ConcurrentHashMap.
 * Every mutation is written to the WAL BEFORE being applied in memory.
 *
 * WHY ConcurrentHashMap?
 *   It allows multiple threads to read/write simultaneously without a single
 *   global lock — much better throughput under concurrent load.
 *
 * Crash recovery flow:
 *   1. On startup, @PostConstruct calls wal.replay()
 *   2. The WAL replays all logged operations in order
 *   3. The resulting map is loaded into memory
 *   → Node resumes exactly where it left off before the crash
 */
@Service
public class StorageService {

    private static final Logger log = Logger.getLogger(StorageService.class.getName());

    private final WriteAheadLog wal;

    // Thread-safe in-memory store
    private final ConcurrentHashMap<String, String> store = new ConcurrentHashMap<>();

    public StorageService(WriteAheadLog wal) {
        this.wal = wal;
    }

    @PostConstruct
    public void recoverFromWal() {
        try {
            Map<String, String> recovered = wal.replay();
            store.putAll(recovered);
            log.info("Recovered " + recovered.size() + " keys from WAL");
        } catch (Exception e) {
            log.warning("WAL recovery failed (starting fresh): " + e.getMessage());
        }
    }

    /**
     * Store a key-value pair.
     * WAL is flushed to disk before the in-memory update.
     */
    public void put(String key, String value) {
        wal.logPut(key, value);   // 1. persist intent
        store.put(key, value);    // 2. apply in memory
    }

    public Optional<String> get(String key) {
        return Optional.ofNullable(store.get(key));
    }

    public boolean delete(String key) {
        if (!store.containsKey(key)) return false;
        wal.logDelete(key);
        store.remove(key);
        return true;
    }

    public Map<String, String> getAll() {
        return Map.copyOf(store);
    }

    public int size() {
        return store.size();
    }
}
