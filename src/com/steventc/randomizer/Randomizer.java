package com.steventc.randomizer;

import com.steventc.shared.Config;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.steventc.shared.Config.BUFFER_SIZE;
import static com.steventc.shared.Config.RUN_TIME_SECONDS;

/**
 * This class will write positive integers into a memory mapped file numbers.dat (can be on /dev/shm if possible)
 * It will then read the responses back from a file called primes.dat, which will include all the numbers followed by T / F if prime or not.
 * <p>
 * This file should really be used like a ring buffer, which some additional level of implementation would be needed on top.
 * <p>
 * A thread will write these outgoing numbers to a file, and another thread will print them. The program will stop after a predetermined number of seconds in the {@link Config} class.
 */
public class Randomizer {

    private final Random randomNumberGenerator = new Random();
    private FileChannel channel;
    private volatile boolean stop = false;
    private volatile boolean completed = false;

    public static void main(String[] args) throws IOException {

        ScheduledExecutorService executor = Executors.newScheduledThreadPool(3);

        Randomizer randomizer = new Randomizer();
        var intBuffer = randomizer.createIntBuffer(Config.BUFFER_SIZE);

        executor.schedule(randomizer::stop, RUN_TIME_SECONDS, TimeUnit.SECONDS);
        executor.submit(() -> randomizer.generateNumber(intBuffer));
        executor.submit(randomizer::printResults);

        executor.shutdown();

    }


    private void generateNumber(IntBuffer intBuffer) {
        randomNumberGenerator.ints(1, Integer.MAX_VALUE)
                .takeWhile(value -> !stop).
                forEach(i -> {
                    if (!intBuffer.hasRemaining()) {
                        intBuffer.rewind();
                    }

                    intBuffer.put(i);
                });

        writeTerminator(intBuffer);
    }

    private void writeTerminator(IntBuffer intBuffer) {
        intBuffer.put(Integer.MIN_VALUE);
        this.completed = true;
    }

    private void printResults() {
        try {
            ByteBuffer resultsReader = this.createResultsReader();

            while (resultsReader.hasRemaining()){
                int potentialPrime = resultsReader.getInt();
                boolean isPrime = (char) resultsReader.getInt() == 'T';

                if (potentialPrime != 0) {
                    System.out.println(potentialPrime + " " + isPrime);
                }

                if (resultsReader.position() == resultsReader.limit()) {
                    resultsReader.rewind();
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public void stop()  {
        this.stop = true;

        while (!completed) {
            //busy loop until done
            System.out.println("Waiting for stream to close...");
        }

        try {
            this.channel.close();
            System.out.println("File channel closed");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public ByteBuffer createResultsReader() throws IOException {
        while (!Files.exists(Paths.get(Config.PRIMES_FILE_PATH))) {
            //wait to load file
        }
        FileChannel channel = FileChannel.open( Paths.get(Config.PRIMES_FILE_PATH), StandardOpenOption.READ, StandardOpenOption.WRITE);
        return channel.map( FileChannel.MapMode.READ_WRITE, 0, BUFFER_SIZE ).asReadOnlyBuffer();
    }

    public IntBuffer createIntBuffer(int size) throws IOException {
        this.channel = FileChannel.open(Paths.get(Config.INTEGER_FILE_PATH), StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
        MappedByteBuffer mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
        return mappedBuffer.asIntBuffer();
    }

}
