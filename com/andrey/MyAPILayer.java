package com.andrey;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpExchange;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class MyAPILayer {
    private final HttpServer server;
    private final MyStorageEngine storageEngine;
    
    public MyAPILayer(int port) throws IOException {
        server = HttpServer.create(new InetSocketAddress(port), 0);
        storageEngine = new MyStorageEngine();
        server.createContext("/ping", new PingHandler());
        server.createContext("/put", new PutHandler());
        server.createContext("/putbatch", new PutBatchHandler());
        server.createContext("/get", new GetHandler());
        server.createContext("/getbatch", new GetBatchHandler());
        server.createContext("/delete", new DeleteHandler());
        server.createContext("/shutdown", new ShutdownHandler());
        server.setExecutor(null);
    }
    
    public void start() {
        server.start();
        System.out.println("Server started on port " + server.getAddress().getPort());
    }
    
    private static Map<String, String> parseQueryParams(String query) {
        Map<String, String> params = new HashMap<>();
        if (query != null) {
            String[] pairs = query.split("&");
            for (String pair : pairs) {
                String[] keyValue = pair.split("=");
                if (keyValue.length == 2) {
                    params.put(keyValue[0], keyValue[1]);
                }
            }
        }
        return params;
    }

    static class PingHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            String response = "pong";
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }
    }

    class ShutdownHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            System.out.println("Shutting down...");
            storageEngine.gracefulClose();
            server.stop(0);
            System.exit(0);
        }
    }
    
    class PutHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            String query = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))
                .lines().collect(Collectors.joining());
            Map<String, String> params = parseQueryParams(query);
            
            String key = params.get("key");
            String value = params.get("value");
            
            if (key == null || value == null) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }

            storageEngine.put(key, value);
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        }
    }
    
    class GetHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            Map<String, String> params = parseQueryParams(
                exchange.getRequestURI().getQuery()
            );
            
            String key = params.get("key");
            if (key == null) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }

            String value = storageEngine.get(key);
            if (value == null) {
                exchange.sendResponseHeaders(404, -1);
                return;
            }

            exchange.sendResponseHeaders(200, value.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(value.getBytes());
            }
        }
    }
    
    class DeleteHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            String query = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))
                .lines().collect(Collectors.joining());
            Map<String, String> params = parseQueryParams(query);
            
            String key = params.get("key");
            if (key == null) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }

            storageEngine.delete(key);
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        }
    }

    class GetBatchHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"GET".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            Map<String, String> params = parseQueryParams(
                exchange.getRequestURI().getQuery()
            );
            
            String keyStart = params.get("keyStart");
            String keyEnd = params.get("keyEnd");
            
            if (keyStart == null || keyEnd == null) {
                exchange.sendResponseHeaders(400, -1);
                return;
            }

            Map<String, String> results = storageEngine.getBatch(keyStart, keyEnd);
            String response = formatBatchResponse(results);
            
            exchange.getResponseHeaders().set("Content-Type", "application/json");
            exchange.sendResponseHeaders(200, response.length());
            try (OutputStream os = exchange.getResponseBody()) {
                os.write(response.getBytes());
            }
        }

        private String formatBatchResponse(Map<String, String> results) {
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            boolean first = true;
            for (Map.Entry<String, String> entry : results.entrySet()) {
                if (!first) {
                    sb.append(",");
                }
                sb.append("\"").append(entry.getKey()).append("\":\"")
                  .append(entry.getValue()).append("\"");
                first = false;
            }
            sb.append("}");
            return sb.toString();
        }
    }

    class PutBatchHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                return;
            }

            String body = new BufferedReader(new InputStreamReader(exchange.getRequestBody()))
                .lines().collect(Collectors.joining());
            
            try {
                // Expecting JSON format: {"key1":"value1","key2":"value2",...}
                Map<String, String> entries = parseJsonToMap(body);
                storageEngine.putBatch(entries);
                exchange.sendResponseHeaders(200, -1);
            } catch (Exception e) {
                String response = "Invalid JSON format";
                exchange.sendResponseHeaders(400, response.length());
                try (OutputStream os = exchange.getResponseBody()) {
                    os.write(response.getBytes());
                }
            }
            exchange.close();
        }

        private Map<String, String> parseJsonToMap(String json) {
            Map<String, String> result = new HashMap<>();
            // Simple JSON parsing (you might want to use a proper JSON library in production)
            json = json.trim();
            if (!json.startsWith("{") || !json.endsWith("}")) {
                throw new IllegalArgumentException("Invalid JSON format");
            }
            json = json.substring(1, json.length() - 1);
            String[] pairs = json.split(",");
            for (String pair : pairs) {
                String[] keyValue = pair.split(":", 2);
                if (keyValue.length != 2) continue;
                String key = keyValue[0].trim().replaceAll("\"", "");
                String value = keyValue[1].trim().replaceAll("\"", "");
                result.put(key, value);
            }
            return result;
        }
    }
} 