package ExerciseSix;

import Commom.Conversor;
import Commom.JobFactory;
import Commom.TransactionsConstants;
import ExerciseFive.WeightByGoodAndYearFromBrazil;
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
 * The type Good price per weight.
 */
public class GoodPricePerWeight {

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
                "good-price-per-weight",
                GoodPricePerWeight.class,
                GoodPricePerWeightMapper.class,
                GoodPricePerWeightReducer.class,
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
     * We're going to map the dataset to send to the reducer a structure like [key => weight/price],
     */
    public static class GoodPricePerWeightMapper extends Mapper<Object, Text, Text, DoubleWritable>
    {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            String[] fields = value.toString().split(";");

            Text good = new Text(fields[TransactionsConstants.GOOD]);

            Double weight = getWeight(fields[TransactionsConstants.WEIGHT]);
            Double price = getPrice(fields[TransactionsConstants.PRICE]);

            context.write(good, new DoubleWritable(weight/price));
        }

        private Double getWeight(String weightStr)
        {
            if (weightStr.isEmpty() || weightStr.equals("weight_kg")) {
                return (double) 0;
            }

            return Double.parseDouble(weightStr);
        }

        private Double getPrice(String price)
        {
            if (price.isEmpty() || price.equals("trade_usd")) {
                return (double) 0;
            }

            return Double.parseDouble(price);
        }
    }

    /**
     * Here we're going to find the maximum value
     */
    public static class GoodPricePerWeightReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            double max = Integer.MIN_VALUE;

            for (DoubleWritable value : values) {
                max = Math.max(value.get(), max);
            }

            context.write(key, new DoubleWritable(max));
        }
    }
}
