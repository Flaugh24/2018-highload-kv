package ru.mail.polis.gagarkin;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.io.Serializable;

@Getter
@AllArgsConstructor
public class Value implements Serializable {
    private final byte[] data;
    private final long timestamp;
    private final State state;

    public enum State {
        PRESENT,
        REMOVED,
        ABSENT
    }
}

class CustomSerializer implements Serializer<Value>, Serializable {

    @Override
    public void serialize(@NotNull DataOutput2 out, @NotNull Value value) throws IOException {
        out.writeInt(value.getData().length);
        out.write(value.getData());
        out.writeLong(value.getTimestamp());
        out.writeUTF(value.getState().name());
    }

    @Override
    public Value deserialize(@NotNull DataInput2 input, int available) throws IOException {
        int length = input.readInt();
        byte[] data = new byte[length];
        input.readFully(data);

        long timestamp = input.readLong();

        Value.State state = Value.State.valueOf(input.readUTF());

        return new Value(data, timestamp, state);
    }

}
