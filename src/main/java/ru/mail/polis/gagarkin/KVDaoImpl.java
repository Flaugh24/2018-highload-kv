package ru.mail.polis.gagarkin;

import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.NoSuchElementException;

public class KVDaoImpl implements KVDao {

    @NotNull
    private final String storage;

    public KVDaoImpl(@NotNull File dir) {
        this.storage = dir.getAbsolutePath();
    }

    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IOException {
        try {
            return Files.readAllBytes(pathByKey(key));
        } catch (NoSuchFileException e) {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) throws IOException {
        Files.write(pathByKey(key), value);
    }

    @Override
    public void remove(@NotNull byte[] key) {
        try {
            Files.delete(pathByKey(key));
        } catch (IOException e) {
        }
    }

    @Override
    public void close() throws IOException {
    }

    @NotNull
    private Path pathByKey(byte[] key) {
        return Paths.get(storage, Base64.getUrlEncoder().encodeToString(key));
    }
}
