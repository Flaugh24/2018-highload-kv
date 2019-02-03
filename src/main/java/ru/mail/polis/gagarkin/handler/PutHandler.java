package ru.mail.polis.gagarkin.handler;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.gagarkin.RF;
import ru.mail.polis.gagarkin.util.HttpHeader;
import ru.mail.polis.gagarkin.util.HttpURL;

import java.io.IOException;
import java.util.NoSuchElementException;

import static ru.mail.polis.gagarkin.util.HttpMethod.PUT;

public class PutHandler extends RequestHandler {
    public PutHandler(@NotNull KVDao dao, @NotNull RF rf, String id, byte[] value) {
        super(PUT.name(), dao, rf, id, value);
    }

    @Override
    public Response onProxy() {
        dao.upsert(id.getBytes(), value);
        return new Response(Response.CREATED, Response.EMPTY);
    }

    @Override
    public boolean ifMe() {
        dao.upsert(id.getBytes(), value);
        return true;
    }

    @Override
    public boolean ifNotMe(HttpClient client) throws InterruptedException, PoolException, HttpException, IOException, NoSuchElementException {
        final Response response = client.put(HttpURL.ENTITY_PATH + "?id=" + id, value, HttpHeader.PROXY_HEADER);
        return response.getStatus() == 201;
    }

    @Override
    public Response onSuccess(int acks) {
        return success(Response.CREATED, acks, Response.EMPTY);
    }

    @Override
    public Response onFail(int acks) {
        return gatewayTimeout(acks);
    }

    @Override
    public Response getResponse(int acks) {
        return super.getResponse(acks);
    }
}
