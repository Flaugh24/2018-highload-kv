package ru.mail.polis.gagarkin.handler;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.KVDao;
import ru.mail.polis.gagarkin.KVDaoImpl;
import ru.mail.polis.gagarkin.RF;

import java.io.IOException;
import java.util.NoSuchElementException;

public abstract class RequestHandler {
    final String methodName;
    @NotNull
    final KVDaoImpl dao;
    final String id;
    final byte[] value;
    final String TIMESTAMP = "timestamp";
    final String STATE = "state";
    @NotNull
    private final RF rf;
    private Logger log = LoggerFactory.getLogger(RequestHandler.class);

    RequestHandler(String methodName, @NotNull KVDao dao, @NotNull RF rf, String id, byte[] value) {
        this.methodName = methodName;
        this.dao = (KVDaoImpl) dao;
        this.rf = rf;
        this.id = id;
        this.value = value;
    }

    public abstract Response onProxy() throws NoSuchElementException;

    public abstract boolean ifMe() throws IOException;

    public abstract boolean ifNotMe(HttpClient client) throws InterruptedException, PoolException, HttpException, IOException, NoSuchElementException;

    abstract Response onSuccess(int acks);

    abstract Response onFail(int acks);

    public Response getResponse(int acks) {
        if (acks >= rf.getAck()) {
            return onSuccess(acks);
        } else {
            return onFail(acks);
        }
    }

    Response gatewayTimeout(int acks) {
        log.info("Operation " + methodName + " failed, acks = " + acks + " ; quorum - " + rf);
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    Response success(String responseName, int acks, byte[] body) {
        log.info("Operation " + methodName + " success " + acks + " nodes; quorum - " + rf);
        return new Response(responseName, body);
    }
}
