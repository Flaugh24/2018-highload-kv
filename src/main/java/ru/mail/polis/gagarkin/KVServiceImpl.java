package ru.mail.polis.gagarkin;


import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.util.NoSuchElementException;

public class KVServiceImpl extends HttpServer implements KVService {

    @NotNull
    private final KVDao dao;

    private final String ENTITY_PREFIX = "id=";

    public KVServiceImpl(int port, @NotNull KVDao dao) throws IOException {
        super(config(port));
        this.dao = dao;
    }

    private static HttpServerConfig config(final int port) {
        final AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;
        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ac};
        return config;
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        Response response = new Response(Response.BAD_REQUEST, Response.EMPTY);
        session.sendResponse(response);
    }

    @Path("/v0/status")
    public Response handleRequestStatus() {
        return Response.ok("ONLINE");
    }

    @Path("/v0/entity")
    public void handleRequestEntity(@NotNull final Request request, @NotNull final HttpSession session) throws IOException {
        final String entityId = request.getParameter(ENTITY_PREFIX);
        if (entityId == null || entityId.isEmpty()) {
            session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
            return;
        }
        switch (request.getMethod()) {
            case Request.METHOD_GET:
                handleRequestGetEntity(session, entityId);
                break;
            case Request.METHOD_PUT:
                handleRequestPutEntity(request, session, entityId);
                break;
            case Request.METHOD_DELETE:
                handleRequestDeleteEntity(session, entityId);
                break;
            default:
                session.sendResponse(new Response(Response.BAD_REQUEST, Response.EMPTY));
        }
    }

    private void handleRequestGetEntity(HttpSession session, String entityId) throws IOException {
        try {
            final byte[] data = dao.get(entityId.getBytes());
            session.sendResponse(Response.ok(data));
        } catch (NoSuchElementException e) {
            session.sendResponse(new Response(Response.NOT_FOUND, Response.EMPTY));
        }
    }

    private void handleRequestPutEntity(Request request, HttpSession session, String entityId) throws IOException {
        dao.upsert(entityId.getBytes(), request.getBody());
        session.sendResponse(new Response(Response.CREATED, Response.EMPTY));
    }

    private void handleRequestDeleteEntity(HttpSession session, String entityId) throws IOException {
        dao.remove(entityId.getBytes());
        session.sendResponse(new Response(Response.ACCEPTED, Response.EMPTY));
    }
}
