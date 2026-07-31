package com.kvstore.controller;

import com.kvstore.service.RouterService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

/**
 * KvController — public REST API
 *
 * GET    /kv/{key}        → retrieve a value
 * PUT    /kv/{key}        → store a value (body = plain text value)
 * DELETE /kv/{key}        → remove a key
 *
 * Every request goes through RouterService which either handles it locally
 * or forwards it to the correct node transparently.
 */
@RestController
@RequestMapping("/kv")
public class KvController {

    private final RouterService routerService;

    public KvController(RouterService routerService) {
        this.routerService = routerService;
    }

    @GetMapping("/{key}")
    public ResponseEntity<String> get(@PathVariable String key) {
        Optional<String> value = routerService.get(key);
        return value.map(ResponseEntity::ok)
                    .orElse(ResponseEntity.notFound().build());
    }

    @PutMapping(value = "/{key}", consumes = "text/plain")
    public ResponseEntity<String> put(@PathVariable String key,
                                      @RequestBody String value) {
        routerService.put(key, value.trim());
        return ResponseEntity.ok("Stored");
    }

    @DeleteMapping("/{key}")
    public ResponseEntity<String> delete(@PathVariable String key) {
        boolean deleted = routerService.delete(key);
        return deleted ? ResponseEntity.ok("Deleted")
                       : ResponseEntity.notFound().build();
    }
}
