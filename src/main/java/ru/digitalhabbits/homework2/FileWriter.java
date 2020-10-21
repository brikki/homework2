package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Exchanger;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {

    private static final Logger logger = getLogger(FileWriter.class);
    private String resultFileName;
    private Exchanger<List<Pair<String, Integer>>> exchanger;

    public FileWriter(String resultFileName, Exchanger<List<Pair<String, Integer>>> exchanger) {
        this.resultFileName = resultFileName;
        this.exchanger = exchanger;
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());
        try(final java.io.FileWriter writer = new java.io.FileWriter(resultFileName)) {
            while (!currentThread().isInterrupted()) {
                List<Pair<String, Integer>> pairs = exchanger.exchange(null);
                for (Pair<String, Integer> pair : pairs) {
                    writer.write(pair.getLeft() + " " + pair.getRight() + "\n");
                }
                writer.flush();
            }
        } catch(IOException | InterruptedException e){
            throw new RuntimeException(e);
        }
        logger.info("Finish writer thread {}", currentThread().getName());
    }
}

