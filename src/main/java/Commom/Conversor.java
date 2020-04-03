package Commom;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

/**
 * The type Conversor.
 */
public class Conversor {

    /**
     * To double writeable double writable.
     *
     * @param value the value
     * @return the double writable
     */
    public static DoubleWritable toDoubleWriteable(double value) { return new DoubleWritable(value); }

    /**
     * To int writeable int writable.
     *
     * @param value the value
     * @return the int writable
     */
    public static IntWritable toIntWriteable(int value) { return new IntWritable(value); }
}
