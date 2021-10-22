package com.steventc.prime;

import com.steventc.shared.Config;
import com.steventc.shared.NumberPrimeContainer;

import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Set;
import java.util.concurrent.*;

import static com.steventc.shared.Config.BUFFER_SIZE;
import static com.steventc.shared.Config.RUN_TIME_SECONDS;
import static com.steventc.shared.IoUtil.resetBuffer;

/**
 * This class is has a reader thread that will poll the memory mapped file for numbers coming from Randomizer.
 * These numbers will be put on a queue pending prime detection. If a prime is already known it will skip this step, and send the result to be written.
 * <p>
 * A number of worker threads will then read from the queue, and check if the numbers are prime or not. The result of this will then be added to another pending write queue.
 * <p>
 * A final thread will read this queue and send the results to be written to another memory mapped file that will be read by the Randomizer.
 * <p>
 * Calculating a prime may not be too CPU intensive for this to be worthwhile, so a regular implementation is provided but is unused. However, this model could work if there was something more intensive than prime calculation as the CPU intensive logic.
 */
public class PrimeRandomizer {

    private volatile boolean stop = false;

    //This could be a primitive collection from fastutil eg: IntSet, or a Bloomfilter from guava
    private final Set<Integer> PRIME_NUMBERS = ConcurrentHashMap.newKeySet();
    private final LinkedBlockingQueue<Integer> numbersToCheckIfPrime = new LinkedBlockingQueue<>();
    private final LinkedBlockingQueue<NumberPrimeContainer> numbersToWrite = new LinkedBlockingQueue<>();  //Could pool NumberPrimeContainer to avoid allocation

    public static void main(String[] args) throws IOException {

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(6);

        var primeRandomizer = new PrimeRandomizer();
        var readableIntBuffer = primeRandomizer.createReadableIntBuffer();
        var writablePrimeBuffer = primeRandomizer.createWritablePrimeBuffer();


        //Reader thread for incoming numbers
        scheduledExecutorService.submit(() -> primeRandomizer.convertIntToPrime(readableIntBuffer));

        //Write thread for out going numbers (some may be prime)
        scheduledExecutorService.submit(() -> primeRandomizer.writeResultsToBuffer(writablePrimeBuffer));

        //A few threads working on calculating non cached if numbers are actually primes
        scheduledExecutorService.submit(primeRandomizer::checkNumberIsPrime);
        scheduledExecutorService.submit(primeRandomizer::checkNumberIsPrime);
        scheduledExecutorService.submit(primeRandomizer::checkNumberIsPrime);


        scheduledExecutorService.schedule(primeRandomizer::stop, RUN_TIME_SECONDS, TimeUnit.SECONDS);

        scheduledExecutorService.shutdown();


    }

    //Maybe expensive, so have it run in a thread
    private void checkNumberIsPrime() {
        while (!stop) {
            if (!numbersToCheckIfPrime.isEmpty()){
                try {
                    var potentialPrime = numbersToCheckIfPrime.take();
                    boolean isPrime = PrimeUtil.isPrime(potentialPrime);
                    if (isPrime) {
                        PRIME_NUMBERS.add(potentialPrime);
                    }
                    numbersToWrite.offer(new NumberPrimeContainer(potentialPrime,isPrime));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void writeResultsToBuffer(MappedByteBuffer buffer) {
        while (!stop) {
            if (!numbersToWrite.isEmpty()) {
                try {
                    NumberPrimeContainer container = numbersToWrite.take();
                    writeNumber(container.number(), container.isPrime(), buffer);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    private void convertIntToPrime(IntBuffer readableIntBuffer) {

        while (readableIntBuffer.hasRemaining()) {

            var i = readableIntBuffer.get();

            if (i == Integer.MIN_VALUE) {
                System.out.println("Stopping!");
                return;
            }

            if (PRIME_NUMBERS.contains(i)){
                numbersToWrite.offer(new NumberPrimeContainer(i, true));
            }else {
                if (i != 0) {
                    numbersToCheckIfPrime.offer(i);
                }
            }

            resetBuffer(readableIntBuffer);

        }

    }


    public void stop() {
        this.stop = true;
    }

    public void writeNumber(int number, boolean isPrime, MappedByteBuffer buffer) {
        if (buffer.limit() == buffer.position()) {
            buffer.rewind();
        }
        buffer.putInt(number);
        buffer.putInt(isPrime ? 'T' : 'F');
    }

    private MappedByteBuffer createWritablePrimeBuffer() throws IOException {
        var channel = FileChannel.open(Paths.get(Config.PRIMES_FILE_PATH), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING);
        return channel.map(FileChannel.MapMode.READ_WRITE, 0, BUFFER_SIZE);
    }

    public IntBuffer createReadableIntBuffer() throws IOException {
        var channel = FileChannel.open(Paths.get(Config.INTEGER_FILE_PATH), StandardOpenOption.READ);
        var mappedByteBuffer = channel.map(FileChannel.MapMode.READ_ONLY, 0, BUFFER_SIZE);
        return mappedByteBuffer.asIntBuffer().asReadOnlyBuffer();
    }

    //Non multithreaded approach, unused.
    private void convertIntToPrime(IntBuffer readableIntBuffer, MappedByteBuffer writablePrimeBuffer) {

        while (readableIntBuffer.hasRemaining()) {

            int i = readableIntBuffer.get();

            if (i == Integer.MIN_VALUE) {
                System.out.println("Stopping!");
                return;
            }

            if (PRIME_NUMBERS.contains(i)){
                System.out.println("Cached number is prime!" + i);
                writeNumber(i, true, writablePrimeBuffer);
            }else {
                boolean prime = PrimeUtil.isPrime(i);
                if (prime){
                    PRIME_NUMBERS.add(i);
                    System.out.println("Found new prime number!" + i);
                    writeNumber(i, true, writablePrimeBuffer);
                }else {
                    writeNumber(i, false, writablePrimeBuffer);
                }
            }

            resetBuffer(readableIntBuffer);

        }

    }


}
