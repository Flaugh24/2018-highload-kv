package ru.mail.polis.gagarkin;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.*;
import java.util.Base64;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    @NotNull
    private final File dir;

    public KVDaoImpl(@NotNull File dir) {
        this.dir = dir;
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {

        final File file = file(key);

        if(!file.exists())
            throw new NoSuchElementException("file does not exist");

        byte[] bytes = new byte[(int) file.length()];
        try (InputStream is = new FileInputStream(file)) {
            if (is.read(bytes) != bytes.length)
                throw new IOException("Can't read file");
        }
        return bytes;
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        try (OutputStream os = new FileOutputStream(file(key))) {
            os.write(value);
        }
    }

    @Override
    public void remove(@NotNull byte[] key) throws IOException {
        file(key).delete();
    }

    @Override
    public void close() throws IOException {
        dir.delete();
    }

    @NotNull
    private File file(byte[] key) {

        return new File(dir, Base64.getEncoder().encodeToString(key).replace("\\","").replace("/", ""));
    }
}
