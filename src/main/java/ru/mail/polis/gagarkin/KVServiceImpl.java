package ru.mail.polis.gagarkin;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import ru.mail.polis.gagarkin.util.HttpHeader;
import ru.mail.polis.gagarkin.util.HttpMethod;
import ru.mail.polis.gagarkin.handler.DeleteHandler;
import ru.mail.polis.gagarkin.handler.GetHandler;
import ru.mail.polis.gagarkin.handler.PutHandler;
import ru.mail.polis.gagarkin.handler.RequestHandler;
import ru.mail.polis.gagarkin.util.HttpURL;

import java.io.*;
import java.util.*;
import java.util.stream.Collectors;


public class KVServiceImpl extends HttpServer implements KVService {


    private final int port;
    @NotNull
    private final KVDaoImpl dao;
    @NotNull
    private final String[] topology;
    private final String me;
    private final RF defaultRF;
    private final Map<String, HttpClient> clients;

    private static final Logger LOG = LoggerFactory.getLogger(KVServiceImpl.class);

    public KVServiceImpl(final int port,
                         @NotNull final KVDao dao,
                         @NotNull final Set<String> topology) throws IOException {
        super(create(port));
        this.port = port;
        this.dao = (KVDaoImpl) dao;
        this.topology = topology.toArray(new String[0]);
        defaultRF = new RF(this.topology.length / 2 + 1, this.topology.length);
        clients = topology
                .stream()
                .filter(node -> !node.endsWith(String.valueOf(port)))
                .collect(Collectors.toMap(
                        o -> o,
                        o -> new HttpClient(new ConnectionString(o))));

        me = Arrays
                .stream(this.topology)
                .filter(node -> node.endsWith(String.valueOf(port)))
                .findFirst()
                .orElseThrow(() -> {
                    IOException e = new IOException("The server not found in the topology " + Arrays.toString(this.topology));
                    LOG.info(e.getMessage(), e);
                    return e;
                });

        String s = "The server on the port %s has been started";
        LOG.info(String.format(s, port));
    }

    private static HttpServerConfig create(int port) {
        AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;

        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ac};
        return config;
    }

    @Path(HttpURL.STATUS_PATH)
    public Response status() {
        return Response.ok("OK");
    }

    @Path(HttpURL.ENTITY_PATH)
    public void handler(Request request,
                        HttpSession session,
                        @Param("id=") String id,
                        @Param("replicas=") String replicas) throws IOException {
        LOG.info("Request parameters:\n" + request);

        if (id == null || id.isEmpty()) {
            LOG.error("id = " + id + " does not meet the requirements");
            session.sendError(Response.BAD_REQUEST, null);
            return;
        }

        RF rf;
        if (replicas == null || replicas.isEmpty()) {
            rf = defaultRF;
        } else {
            try {
                rf = new RF(replicas);
            } catch (IllegalArgumentException e) {
                LOG.error(e.getMessage(), e);
                session.sendError(Response.BAD_REQUEST, null);
                return;
            }
        }

        String[] nodes;
        try {
            nodes = replicas(id, rf.getFrom());
        } catch (IllegalArgumentException e) {
            LOG.error(e.getMessage(), e);
            session.sendError(Response.BAD_REQUEST, null);
            return;
        }

        boolean proxied = request.getHeader(HttpHeader.PROXY_HEADER) != null;
        LOG.info("Request type - " + getMethodName(request.getMethod()) + "; proxied - " + proxied);
        try {
            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    session.sendResponse(customHandler(new GetHandler(dao, rf, id), nodes, proxied));
                    return;
                case Request.METHOD_PUT:
                    session.sendResponse(customHandler(new PutHandler(dao, rf, id, request.getBody()), nodes, proxied));
                    return;
                case Request.METHOD_DELETE:
                    session.sendResponse(customHandler(new DeleteHandler(dao, rf, id), nodes, proxied));
                    return;
                default:
                    LOG.error(request.getMethod() + " Unsupported request");
                    session.sendResponse(new Response(Response.METHOD_NOT_ALLOWED));
            }
        } catch (NoSuchElementException e) {
            LOG.info("Item by key " + id + " not found", e);
            session.sendError(Response.NOT_FOUND, null);
        } catch (IOException e) {
            LOG.error("Internal Server Error", e);
            LOG.error("Request parameters:\n" + request);
            session.sendError(Response.INTERNAL_ERROR, null);
        }
    }

    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        LOG.error("Unsupported request\n" + request);
        session.sendError(Response.BAD_REQUEST, null);
    }

    private Response customHandler(RequestHandler rh, String[] nodes, boolean proxied) throws IOException, NoSuchElementException {
        if (proxied) {
            return rh.onProxy();
        }

        int acks = 0;
        for (final String node : nodes) {
            if (node.equals(me)) {
                if (rh.ifMe()) {
                    acks++;
                }
            } else {
                try {
                    if (rh.ifNotMe(clients.get(node))) {
                        acks++;
                    }
                } catch (InterruptedException | PoolException | HttpException e) {
                    LOG.error(e.getMessage(), e);
                }
            }
        }
        return rh.getResponse(acks);
    }

    private String[] replicas(String id, int count) throws IllegalArgumentException {
        if (count > topology.length) {
            throw new IllegalArgumentException("The from value must be less or equal to the total count of nodes = " + topology.length);
        }
        String[] result = new String[count];
        int i = (id.hashCode() & Integer.MAX_VALUE) % topology.length;
        for (int j = 0; j < count; j++) {
            result[j] = topology[i];
            i = (i + 1) % topology.length;
        }
        return result;
    }

    private String getMethodName(int method) {

        switch (method) {
            case Request.METHOD_GET:
                return HttpMethod.GET.name();
            case Request.METHOD_PUT:
                return HttpMethod.PUT.name();
            case Request.METHOD_DELETE:
                return HttpMethod.DELETE.name();
            default:
                return "UNSUPPORTED " + method;
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();

        String s = "Server on port %s was stopped";
        LOG.info(String.format(s, port));
    }

}
