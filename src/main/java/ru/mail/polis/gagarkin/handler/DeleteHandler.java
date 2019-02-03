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

import static ru.mail.polis.gagarkin.util.HttpMethod.DELETE;


public class DeleteHandler extends RequestHandler {

    public DeleteHandler(@NotNull KVDao dao, @NotNull RF rf, String id) {
        super(DELETE.name(), dao, rf, id, null);
    }

    @Override
    public Response onProxy() {
        dao.remove(id.getBytes());
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    @Override
    public boolean ifMe() {
        dao.remove(id.getBytes());
        return true;
    }

    @Override
    public boolean ifNotMe(HttpClient client) throws InterruptedException, PoolException, HttpException, IOException {
        final Response response = client.delete(HttpURL.ENTITY_PATH + "?id=" + id, HttpHeader.PROXY_HEADER);
        return response.getStatus() == 202;
    }

    @Override
    public Response onSuccess(int acks) {
        return success(Response.ACCEPTED, acks, Response.EMPTY);
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
