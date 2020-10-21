package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.Exchanger;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    private static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();


    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);
        final File file = new File(processingFileName);
        final Phaser phaser = new Phaser(CHUNK_SIZE + 1);
        ExecutorService executorService = Executors.newFixedThreadPool(CHUNK_SIZE);
        final Exchanger<List<Pair<String, Integer>>> exchanger = new Exchanger<>();
        Thread fileWriteThread = new Thread(new FileWriter(resultFileName, exchanger));
        fileWriteThread.start();
        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {
                final List<String> lines = new ArrayList<>();
                final List<Pair<String, Integer>> resultLine = new ArrayList<>();
                while (lines.size() < CHUNK_SIZE && scanner.hasNextLine()) {
                    String string = scanner.nextLine();
                    lines.add(string);
                }
                if (lines.size() >= CHUNK_SIZE) {
                    phaser.arriveAndDeregister();
                }
                for (int i = 0; i < lines.size(); i++) {
                    int finalI = i;
                    executorService.submit(() -> {
                        resultLine.set(finalI, new LineCounterProcessor().process(lines.get(finalI)));
                        phaser.arrive();
                    });
                }
                phaser.arriveAndAwaitAdvance();
                exchanger.exchange(resultLine);
            }
        } catch (IOException | InterruptedException exception) {
            logger.error("", exception);
        }
        fileWriteThread.interrupt();
        executorService.shutdown();
        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }
}
