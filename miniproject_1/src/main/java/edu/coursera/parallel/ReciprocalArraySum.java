package edu.coursera.parallel;
import java.util.*; 
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

/**
 * Class wrapping methods for implementing reciprocal array sum in parallel.
 */
public final class ReciprocalArraySum {

    /**
     * Default constructor.
     */
    private ReciprocalArraySum() {
    }

    /**
     * Sequentially compute the sum of the reciprocal values for a given array.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double seqArraySum(final double[] input) {
        double sum = 0;

        // Compute sum of reciprocals of array elements
        for (int i = 0; i < input.length; i++) {
            sum += 1 / input[i];
        }

        return sum;
    }

    /**
     * Computes the size of each chunk, given the number of chunks to create
     * across a given number of elements.
     *
     * @param nChunks The number of chunks to create
     * @param nElements The number of elements to chunk across
     * @return The default chunk size
     */
    private static int getChunkSize(final int nChunks, final int nElements) {
        // Integer ceil
        return (nElements + nChunks - 1) / nChunks;
    }

    /**
     * Computes the inclusive element index that the provided chunk starts at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the start of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The inclusive index that this chunk starts at in the set of
     *         nElements
     */
    private static int getChunkStartInclusive(final int chunk,
            final int nChunks, final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        return chunk * chunkSize;
    }

    /**
     * Computes the exclusive element index that the provided chunk ends at,
     * given there are a certain number of chunks.
     *
     * @param chunk The chunk to compute the end of
     * @param nChunks The number of chunks created
     * @param nElements The number of elements to chunk across
     * @return The exclusive end index for this chunk
     */
    private static int getChunkEndExclusive(final int chunk, final int nChunks,
            final int nElements) {
        final int chunkSize = getChunkSize(nChunks, nElements);
        final int end = (chunk + 1) * chunkSize;
        if (end > nElements) {
            return nElements;
        } else {
            return end;
        }
    }

    /**
     * This class stub can be filled in to implement the body of each task
     * created to perform reciprocal array sum in parallel.
     */
    private static class ReciprocalArraySumTask extends RecursiveAction {
        public static int threshold = 1000000; 
        /**
         * Starting index for traversal done by this task.
         */
        private final int startIndexInclusive;
        /**
         * Ending index for traversal done by this task.
         */
        private final int endIndexExclusive;
        /**
         * Input array to reciprocal sum.
         */
        private final double[] input;
        /**
         * Intermediate value produced by this task.
         */
        private double value;

        private int chunk;

        /**
         * Constructor.
         * @param setStartIndexInclusive Set the starting index to begin
         *        parallel traversal at.
         * @param setEndIndexExclusive Set ending index for parallel traversal.
         * @param setInput Input values
         */
        ReciprocalArraySumTask(final int setStartIndexInclusive,
                final int setEndIndexExclusive, final double[] setInput, final int _chunk) {
            this.startIndexInclusive = setStartIndexInclusive;
            this.endIndexExclusive = setEndIndexExclusive;
            this.input = setInput;
            this.chunk = _chunk;
        }

        /**
         * Getter for the value produced by this task.
         * @return Value produced by this task
         */
        public double getValue() {
            return value;
        }

        @Override
        protected void compute() {
            // if (startIndexInclusive != 0 || endIndexExclusive != input.length) {
            if (endIndexExclusive - startIndexInclusive <= threshold) {
                // System.out.format("%d %d\n", startIndexInclusive, endIndexExclusive);
                for (int i = startIndexInclusive; i < endIndexExclusive; ++i) {
                    value += 1 / input[i];
                }
            }
            else {
                // ReciprocalArraySumTask left = new ReciprocalArraySumTask(
                //     startIndexInclusive, 
                //     (startIndexInclusive + endIndexExclusive) / 2,
                //     input);
                // ReciprocalArraySumTask right = new ReciprocalArraySumTask(
                //     (startIndexInclusive + endIndexExclusive) / 2,
                //     endIndexExclusive,
                //     input);

                List<ReciprocalArraySumTask> taskList = new ArrayList<ReciprocalArraySumTask>();

                for (int i = 0; i < chunk; ++i) {
                    int start = startIndexInclusive + getChunkStartInclusive(i, chunk, endIndexExclusive - startIndexInclusive);
                    int end = startIndexInclusive + getChunkEndExclusive(i, chunk, endIndexExclusive - startIndexInclusive);
                    // System.out.format("start: %d, end: %d\n", start, end);
                    taskList.add(new ReciprocalArraySumTask(start, end, input, chunk));
                }
                invokeAll(taskList);
                // left.fork();
                // right.compute();
                // left.join();
                // value = left.getValue() + right.getValue();

                // taskList.get(0).fork();
                // taskList.get(1).fork();
                // // taskList.get(1).compute();
                // taskList.get(0).join();
                // taskList.get(1).join();
                // value = taskList.get(0).getValue() + taskList.get(1).getValue();

                // for (int i = 0; i < taskList.size(); ++i) {
                //     // System.out.printf("fork");
                //     taskList.get(i).fork();
                // }
                for (int i = 0; i < taskList.size(); ++i) {
                    // System.out.printf("join");
                    // taskList.get(i).join();
                    value += taskList.get(i).getValue();
                }
                
            }
        }
    }

    /**
     * TODO: Modify this method to compute the same reciprocal sum as
     * seqArraySum, but use two tasks running in parallel under the Java Fork
     * Join framework. You may assume that the length of the input array is
     * evenly divisible by 2.
     *
     * @param input Input array
     * @return The sum of the reciprocals of the array input
     */
    protected static double parArraySum(final double[] input) {
        assert input.length % 2 == 0;

        // System.setProperties("java.util.concurrent.ForkJoinPool.");
        // create obj here?
        ReciprocalArraySumTask t = new ReciprocalArraySumTask(0, input.length, input, 2);
        // System.setProperty("java.util.concurrent.ForkJoinPool.common.paralellism","2");
        // ForkJoinPool pool = new ForkJoinPool(4);

        // System.out.format("pool parallelism: %d\n", pool.getParallelism());

        // pool.invoke(t);

        ForkJoinPool.commonPool().invoke(t);

        double sum = t.getValue();

        return sum;
    }

    /**
     * TODO: Extend the work you did to implement parArraySum to use a set
     * number of tasks to compute the reciprocal array sum. You may find the
     * above utilities getChunkStartInclusive and getChunkEndExclusive helpful
     * in computing the range of element indices that belong to each chunk.
     *
     * @param input Input array
     * @param numTasks The number of tasks to create
     * @return The sum of the reciprocals of the array input
     */
    protected static double parManyTaskArraySum(final double[] input,
            final int numTasks) {

        // int chunkSize = getChunkSize(numTasks, input.length);
        // ForkJoinPool pool = new ForkJoinPool(chunkSize);

        ReciprocalArraySumTask t = new ReciprocalArraySumTask(0, input.length, input, numTasks);
        // pool.invoke(t);

        
        // for (int nthChunk = 0; nthChunk < numTasks; ++nthChunk) {
        //     int start = getChunkStartInclusive(nthChunk, numTasks, input.length);
        //     int end = getChunkEndExclusive(nthChunk, numTasks, input.length);
        //     taskList.add(new ReciprocalArraySumTask(start, end, input));
        // }

        ForkJoinPool.commonPool().invoke(t);

        double sum = t.getValue();



        return sum;
    }
}
