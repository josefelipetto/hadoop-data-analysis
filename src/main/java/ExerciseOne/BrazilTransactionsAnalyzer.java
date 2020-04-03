package ExerciseOne;

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
 * Solution for exercise one
 */
public class BrazilTransactionsAnalyzer {


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

        Job job = JobFactory.create("brazil-transaction",
                BrazilTransactionsAnalyzer.class,
                TransactionsCountMapper.class,
                TransactionsCountReducer.class,
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
     * Mapper responsible for filter the dataset and send it out to the Reducer.
     * We're filtering the dataset to get only the transactions that were made in Brazil.
     * It's going to be send a key-value pair list to the reducer in the type of good and quantity.
     */
    public static class TransactionsCountMapper extends Mapper<Object, Text, Text, DoubleWritable>
    {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(";");

            String country = fields[TransactionsConstants.COUNTRY];

            if (country.equals("Brazil")) {

                Text good = getGood(fields[TransactionsConstants.GOOD]);
                DoubleWritable quantity = getQuantity(fields[TransactionsConstants.QUANTITY]);

                context.write(good, quantity);
            }
        }

        private Text getGood(String good)
        {
            Text goodText = new Text();
            goodText.set(good);
            return goodText;
        }

        private DoubleWritable getQuantity(String quantity)
        {
            if (quantity.isEmpty()) {
                return new DoubleWritable(0);
            }

            return new DoubleWritable(Double.parseDouble(quantity));
        }
    }

    /**
     * The reducer class responsible for take the key-pair value list and aggregate that.
     * We're going to sum the transactions and pass it along
     */
    public static class TransactionsCountReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
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
