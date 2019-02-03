package ru.mail.polis.gagarkin;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import ru.mail.polis.KVDao;

import java.io.File;
import java.util.NoSuchElementException;


public class KVDaoImpl implements KVDao {

    private final DB db;
    private final HTreeMap<byte[], Value> storage;

    public KVDaoImpl(File data) {
        File dataBase = new File(data, "dataBase");
        Serializer<Value> serializer = new CustomSerializer();
        this.db = DBMaker
                .fileDB(dataBase)
                .fileMmapEnableIfSupported()
                .fileMmapPreclearDisable()
                .fileChannelEnable()
                .closeOnJvmShutdown()
                .make();
        this.storage = db.hashMap(data.getName())
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(serializer)
                .createOrOpen();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IllegalStateException {
        Value value = internalGet(key);

        if (value.getState() == Value.State.ABSENT || value.getState() == Value.State.REMOVED) {
            throw new NoSuchElementException();
        }
        return value.getData();
    }

    @NotNull
    public Value internalGet(@NotNull byte[] key) {
        Value value = storage.get(key);
        if (value == null) {
            return new Value(new byte[]{}, 0, Value.State.ABSENT);
        }
        return value;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        storage.put(key, new Value(value, System.currentTimeMillis(), Value.State.PRESENT));
    }

    @Override
    public void remove(@NotNull byte[] key) {
        storage.put(key, new Value(new byte[]{}, System.currentTimeMillis(), Value.State.REMOVED));
    }

    @Override
    public void close() {
        db.close();
    }
}
