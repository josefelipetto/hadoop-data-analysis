package ExerciseThree;

import Commom.JobFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * The type Best seller good in 2016.
 */
public class BestSellerGoodIn2016 {

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
     * @throws InterruptedException   the interrupted exception
     * @throws IOException            the io exception
     * @throws ClassNotFoundException the class not found exception
     */
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        SumOfSoldGoodsIn2016.main(new String[]{"Input/transactions.csv", "intermediaryOutputEx3"});

        Job job = JobFactory.create("best-seller-good-in-2016",
                BestSellerGoodIn2016.class,
                BestSellerGoodIn2016Mapper.class,
                BestSellerGoodIn2016Reducer.class,
                Text.class,
                DoubleWritable.class,
                new Path("intermediaryOutputEx3/part-r-00000"),
                new Path("outputEx3")
        );

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        Counters counters = job.getCounters();

        System.out.println("Invalid Records: " + counters.findCounter(COUNTERS.INVALID_RECORD_COUNT).getValue());
    }

    /**
     * The type Best seller good in 2016 mapper.
     */
    public static class BestSellerGoodIn2016Mapper extends Mapper<Object, Text, Text, DoubleWritable>
    {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");

            Text good = new Text(fields[0]);
            DoubleWritable quantity = getQuantity(fields[1]);

            context.write(good, quantity);
        }

        private DoubleWritable getQuantity(String quantity)
        {
            if (quantity.isEmpty()) {
                return new DoubleWritable(0);
            }

            return  new DoubleWritable(Double.parseDouble(quantity));
        }
    }

    /**
     * The type Best seller good in 2016 reducer.
     */
    public static class BestSellerGoodIn2016Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {

            double max = 0;

            for (DoubleWritable value: values) {
                max = Math.max(value.get(), max);
            }

            context.write(key, new DoubleWritable(max));
        }
    }
}
