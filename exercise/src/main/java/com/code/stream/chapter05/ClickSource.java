package com.code.stream.chapter05;

import com.code.stream.entity.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class ClickSource implements SourceFunction<Event> {
    private Boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws Exception {
        String[] users = {"Bob", "Tom", "Mary", "Alice"};
        String[] urls = {"./home", "./prod?id=100", "./prod?id=100", "./fav", "./cart"};

        Random random = new Random();

        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            sourceContext.collect(new Event(user, url, System.currentTimeMillis()));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
