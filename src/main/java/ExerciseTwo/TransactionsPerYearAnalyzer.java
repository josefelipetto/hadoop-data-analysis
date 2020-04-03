package ExerciseTwo;

import Commom.Conversor;
import Commom.JobFactory;
import Commom.TransactionsConstants;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

/**
 * Solution for exercise two
 */
public class TransactionsPerYearAnalyzer {

    private enum COUNTERS {
        /**
         * Invalid record count counters.
         */
        INVALID_RECORD_COUNT
    }

    /**
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws IOException            the io exception
     * @throws ClassNotFoundException the class not found exception
     * @throws InterruptedException   the interrupted exception
     */
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        BasicConfigurator.configure();

        Job job = JobFactory.create("transactions-per-year",
                TransactionsPerYearAnalyzer.class,
                TransactionsPerYearMapper.class,
                TransactionsPerYearReducer.class,
                Text.class,
                DoubleWritable.class,
                new Path(args[0]),
                new Path(args[1])
        );

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        Counters counters = job.getCounters();

        System.out.println("Invalid Records: " + counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());
    }

    /**
     * Map the datasets, takes the year and quantity and pass it to the reducer as a key-value pair list.
     * Since we need transactions per year, we're using the year as the key
     */
    public static class TransactionsPerYearMapper extends Mapper<Object, Text, Text, DoubleWritable>
    {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(";");

            Text year = new Text(fields[TransactionsConstants.YEAR]);
            DoubleWritable quantity = new DoubleWritable(1);

            context.write(year, quantity);
        }

    }

    /**
     * Reducer that gets that key-value pair list and aggregates that by get the sumOfTransactions of each year.
     */
    public static class TransactionsPerYearReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            double sumOfTransactions = 0;

            for (DoubleWritable transaction : values) {
                sumOfTransactions += transaction.get();
            }

            context.write(key, Conversor.toDoubleWriteable(sumOfTransactions));
        }
    }


}
