package Commom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * The type Job factory.
 */
public class JobFactory {

    /**
     * Create a job
     *
     * @param name        the name
     * @param jar         the jar
     * @param mapper      the mapper
     * @param reducer     the reducer
     * @param outputKey   the output key
     * @param outputValue the output value
     * @param input       the input
     * @param output      the output
     * @return the job
     * @throws IOException the io exception
     */
    public static Job create(String name,
                             Class jar,
                             Class<?extends Mapper> mapper,
                             Class<? extends Reducer> reducer,
                             Class outputKey,
                             Class outputValue,
                             Path input,
                             Path output
    ) throws IOException
    {
        Configuration configuration = new Configuration();

        Job job = Job.getInstance(configuration, name);

        job.setJarByClass(jar);
        job.setMapperClass(mapper);
        job.setReducerClass(reducer);
        job.setOutputKeyClass(outputKey);
        job.setOutputValueClass(outputValue);

        FileInputFormat.addInputPath(job, input);
        FileOutputFormat.setOutputPath(job, output);

        return job;
    }
}
