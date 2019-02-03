package ru.mail.polis.gagarkin;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

import java.util.StringTokenizer;

@Getter
@ToString
@AllArgsConstructor
public class RF {
    private final String errorMsg = "The replicas parameter must contain 2 non-negative integer separated by a symbol \"/\" -> \"ack/from\"\n" +
            "The ack value must be less than or equal to the value of from";
    private final int ack, from;

    RF(String replicas) throws IllegalArgumentException {
        StringTokenizer st = new StringTokenizer(replicas, "/");
        if (st.countTokens() != 2) {
            throw new IllegalArgumentException(errorMsg);
        }
        try {
            ack = Integer.parseInt(st.nextToken());
            from = Integer.parseInt(st.nextToken());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(errorMsg);
        }
        validate();
    }


    private void validate() throws IllegalArgumentException {
        if (ack > from || ack <= 0) {
            throw new IllegalArgumentException(errorMsg);
        }
    }
}
