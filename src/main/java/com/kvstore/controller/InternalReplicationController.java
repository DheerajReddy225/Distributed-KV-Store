package com.kvstore.controller;

import com.kvstore.service.StorageService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

/**
 * InternalReplicationController
 *
 * This endpoint is called by the ReplicationService on peer nodes.
 * It writes the replicated value DIRECTLY to local storage, bypassing
 * the router (to avoid infinite forwarding loops).
 *
 * In production you'd add an internal auth token to prevent abuse.
 */
@RestController
@RequestMapping("/internal/replicate")
public class InternalReplicationController {

    private final StorageService storageService;

    public InternalReplicationController(StorageService storageService) {
        this.storageService = storageService;
    }

    @PutMapping(value = "/{key}", consumes = "text/plain")
    public ResponseEntity<Void> replicatePut(@PathVariable String key,
                                             @RequestBody String value) {
        storageService.put(key, value.trim());
        return ResponseEntity.ok().build();
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<Void> replicateDelete(@PathVariable String key) {
        storageService.delete(key);
        return ResponseEntity.ok().build();
    }
}
