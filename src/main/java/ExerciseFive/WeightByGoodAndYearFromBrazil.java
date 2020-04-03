package ExerciseFive;

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
 * The type Weight by good and year from brazil.
 */
public class WeightByGoodAndYearFromBrazil {

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
                "weight-by-good-and-year-from-brazil",
                WeightByGoodAndYearFromBrazil.class,
                WeightByGoodAndYearFromBrazilMapper.class,
                WeightByGoodAndYearFromBrazilReducer.class,
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
     * Map the dataset to take only Brazil's transaction and send it out to the Reducer class,
     * using composite key GOOD + YEAR => Weight
     */
    public static class WeightByGoodAndYearFromBrazilMapper extends Mapper<Object, Text, Text, DoubleWritable>
    {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(";");

            if (fields[TransactionsConstants.COUNTRY].equals("Brazil")) {

                Text goodByYear = new Text(fields[TransactionsConstants.GOOD] + " " + fields[TransactionsConstants.YEAR]);

                DoubleWritable weight = getWeight(fields[TransactionsConstants.WEIGHT]);

                context.write(goodByYear, weight);
            }

        }

        private DoubleWritable getWeight(String weightStr)
        {
            if (weightStr.isEmpty() || weightStr.equals("weight_kg")) {
                return new DoubleWritable(0);
            }

            return Conversor.toDoubleWriteable(Double.parseDouble(weightStr));
        }
    }

    /**
     * Reducer will aggregate by getting the sum of weight and dividing by the number of transactions
     */
    public static class WeightByGoodAndYearFromBrazilReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            double sum = 0;
            int numberOfTransactions = 0;

            for (DoubleWritable weight : values) {
                sum += weight.get();
                numberOfTransactions++;
            }

            context.write(key, Conversor.toDoubleWriteable(sum/numberOfTransactions));
        }
    }
}
