package ExerciseSeven;

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
 * The type Amount of transaction per flow and year.
 */
public class AmountOfTransactionPerFlowAndYear {

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

        Job job = JobFactory.create(
                "amount-of-transaction-per-flow-and-year",
                AmountOfTransactionPerFlowAndYear.class,
                AmountOfTransactionPerFlowAndYearMapper.class,
                AmountOfTransactionPerFlowAndYearReducer.class,
                Text.class,
                DoubleWritable.class,
                new Path(args[0]),
                new Path(args[1])
        );

        job.waitForCompletion(true);

        Counters counters = job.getCounters();

        System.out.println("Invalid Records: " + counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());
    }

    /**
     * Create a list with composite key [flow+year => quantity] and send to the reducer
     */
    public static class AmountOfTransactionPerFlowAndYearMapper extends Mapper<Object, Text, Text, DoubleWritable>
    {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(";");

            Text keyFlowYear = new Text(fields[TransactionsConstants.FLOW].concat(fields[TransactionsConstants.YEAR]));
            DoubleWritable quantity = new DoubleWritable(1);

            context.write(keyFlowYear, quantity);
        }
    }

    /**
     * Sum the transactions of each composite key list and pass it along
     */
    public static class AmountOfTransactionPerFlowAndYearReducer extends Reducer<Text, DoubleWritable, Text ,DoubleWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            double quantity = 0;

            for (DoubleWritable value : values) {
                quantity += value.get();
            }

            context.write(key, new DoubleWritable(quantity));
        }
    }
}
