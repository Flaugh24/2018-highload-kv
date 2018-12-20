package ru.mail.polis.gagarkin;


import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.NoSuchElementException;

public class KVServiceImpl implements KVService {

    @NotNull
    private final HttpServer httpServer;
    @NotNull
    private final KVDao dao;

    @NotNull
    private static String extractId(@NotNull final String query) {

        final String PREFIX = "id=";

        if (!query.startsWith("id=")) {
            throw new IllegalArgumentException();
        }

        String id = query.substring(PREFIX.length());
        if(id.isEmpty())
            throw new IllegalArgumentException();

        return id;
    }

    public KVServiceImpl(int port, @NotNull KVDao dao) throws IOException {
        this.dao = dao;
        this.httpServer = HttpServer.create(new InetSocketAddress(port), 0);

        this.httpServer.createContext("/v0/status", http -> {
            http.sendResponseHeaders(200, 0);
            http.close();
        });

        this.httpServer.createContext("/v0/entity", new ErrorHandler(http -> {

                    String id = extractId(http.getRequestURI().getQuery());
                    switch (http.getRequestMethod().toUpperCase()) {
                        case "GET":
                            byte[] getContent = dao.get(id.getBytes());
                            http.sendResponseHeaders(200, getContent.length);
                            http.getResponseBody().write(getContent);
                            break;
                        case "PUT":
                            dao.upsert(id.getBytes(), http.getRequestBody().readAllBytes());
                            http.sendResponseHeaders(201, 0);
                            break;
                        case "DELETE":
                            dao.remove(id.getBytes());
                            http.sendResponseHeaders(202, 0);
                            break;
                        default:
                            http.sendResponseHeaders(405, 0);
                    }
                    http.close();
                })
        );

    }

    @Override
    public void start() {
        this.httpServer.start();
    }

    @Override
    public void stop() {
        this.httpServer.stop(0);
    }

    private static class ErrorHandler implements HttpHandler {

        private final HttpHandler delegate;

        private ErrorHandler(HttpHandler delegate) {
            this.delegate = delegate;
        }

        @Override
        public void handle(HttpExchange exchange) throws IOException {
            try {
                delegate.handle(exchange);
            } catch (NoSuchElementException e) {
                exchange.sendResponseHeaders(404, 0);
                exchange.close();
            }catch (IllegalArgumentException e){
                exchange.sendResponseHeaders(400, 0);
                exchange.close();
            }
        }
    }
}
