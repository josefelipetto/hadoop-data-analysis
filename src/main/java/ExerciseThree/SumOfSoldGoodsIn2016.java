package ExerciseThree;

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
 * The type Sum of sold goods in 2016.
 */
public class SumOfSoldGoodsIn2016 {

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
                "most-selled-good-2016",
                SumOfSoldGoodsIn2016.class,
                SumOfSoldGoodsIn2016Mapper.class,
                SumOfSoldGoodsIn2016Reducer.class,
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
     * Map the dataset by sending a key-value pair list to the reducer of products that were sold in 2016
     */
    public static class SumOfSoldGoodsIn2016Mapper extends Mapper<Object, Text, Text, DoubleWritable>
    {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(";");

            if (fields[TransactionsConstants.YEAR].equals("2016")) {
                Text good = new Text(fields[TransactionsConstants.GOOD]);
                DoubleWritable quantity = new DoubleWritable(1);
                context.write(good, quantity);
            }
        }
    }

    /**
     * Sum the key-value pair list by product
     */
    public static class SumOfSoldGoodsIn2016Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
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
