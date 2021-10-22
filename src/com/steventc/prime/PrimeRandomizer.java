package com.steventc.prime;

import com.steventc.shared.Config;
import com.steventc.shared.NumberPrimeContainer;

import java.io.IOException;
import java.nio.IntBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;

import static com.steventc.shared.Config.BUFFER_SIZE;

public class PrimeRandomizer {

    private volatile boolean stop = false;

    //This could be a primitive collection from fastutil eg: IntSet, or a Bloomfilter from guava
    Set<Integer> PRIME_NUMBERS = ConcurrentHashMap.newKeySet();
    LinkedBlockingQueue<Integer> numbersToCheckIfPrime = new LinkedBlockingQueue<>();
    LinkedBlockingQueue<NumberPrimeContainer> numbersToWrite = new LinkedBlockingQueue<>();  //Could pool NumberPrimeContainer to avoid allocation

    public static void main(String[] args) throws IOException {

        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(5);

        PrimeRandomizer primeRandomizer = new PrimeRandomizer();
        IntBuffer readableIntBuffer = primeRandomizer.createReadableIntBuffer();
        MappedByteBuffer writablePrimeBuffer = primeRandomizer.createWritablePrimeBuffer();
//        primeRandomizer.convertIntToPrime(readableIntBuffer, writablePrimeBuffer);
        ;


        //Reader thread for incoming numbers
        scheduledExecutorService.submit(() -> primeRandomizer.convertIntToPrimeNewThread(readableIntBuffer));

        //Write thread for out going numbers (some may be prime)
        scheduledExecutorService.submit(() -> primeRandomizer.writeNumbers(writablePrimeBuffer));

        //A few threads working on calculating non cached if numbers are actually primes
        scheduledExecutorService.submit(primeRandomizer::checkNumberIsPrime);
        scheduledExecutorService.submit(primeRandomizer::checkNumberIsPrime);
        scheduledExecutorService.submit(primeRandomizer::checkNumberIsPrime);

        scheduledExecutorService.shutdown();


    }

    //Maybe expensive, so have it run in a thread
    private void checkNumberIsPrime() {
        while (!stop) {
            if (!numbersToCheckIfPrime.isEmpty()){
                Integer potentialPrime = null;
                try {
                    potentialPrime = numbersToCheckIfPrime.take();
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

    private void writeNumbers(MappedByteBuffer buffer) {
        while (!stop) {
            if (!numbersToWrite.isEmpty()){
                NumberPrimeContainer container = null;
                try {
                    container = numbersToWrite.take();
                    writeNumber(container.number(), container.isPrime(), buffer);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }
        }
    }

    private void convertIntToPrimeNewThread(IntBuffer readableIntBuffer) {

        while (readableIntBuffer.hasRemaining()) {

            int i = readableIntBuffer.get();

            if (i == Integer.MIN_VALUE) {
                System.out.println("Stopping!");
                return;
            }


            if (PRIME_NUMBERS.contains(i)){
                System.out.println("Cached number is prime!" + i);
                numbersToWrite.offer(new NumberPrimeContainer(i, true));

//                writeNumber(i, true, writablePrimeBuffer);
            }else {

                if (i != 0){
                    numbersToCheckIfPrime.offer(i);
                }


//                boolean prime = PrimeUtil.isPrime(i);
//                if (prime){
//                    PRIME_NUMBERS.add(i);
//                    System.out.println("Found new prime number!" + i);
//                    writeNumber(i, true, writablePrimeBuffer);
//                }else {
//                    writeNumber(i, false, writablePrimeBuffer);
//                }
            }

            if (readableIntBuffer.position() == readableIntBuffer.limit()) {
                readableIntBuffer.rewind();
            }

        }

    }




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

            if (readableIntBuffer.position() == readableIntBuffer.limit()) {
                readableIntBuffer.rewind();
            }

        }

    }

    public void writeNumber(int number, boolean isPrime, MappedByteBuffer buffer) {
        if (buffer.limit() == buffer.position()) {
            buffer.rewind();
        }
        buffer.putInt(number);
        buffer.putInt(isPrime ? 'T' : 'F');
    }

    private MappedByteBuffer createWritablePrimeBuffer() throws IOException {
        FileChannel channel = FileChannel.open( Paths.get(Config.PRIMES_FILE_PATH), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.TRUNCATE_EXISTING);
        return channel.map( FileChannel.MapMode.READ_WRITE, 0, BUFFER_SIZE );
    }

    public IntBuffer createReadableIntBuffer() throws IOException {
        FileChannel channel = FileChannel.open( Paths.get(Config.INTEGER_FILE_PATH), StandardOpenOption.READ);
        MappedByteBuffer mappedByteBuffer = channel.map( FileChannel.MapMode.READ_ONLY, 0, BUFFER_SIZE );
        return mappedByteBuffer.asIntBuffer().asReadOnlyBuffer();
    }


}
