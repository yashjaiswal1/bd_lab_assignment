package sales;

import java.io.IOException;

public class SalesCountryReducer extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce (Text t_key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter report  throws IOException {

        Text key = t_key;

        int frequencyForCountry = 0;
        while (values.hasNext()) {

            // replace type of value with the actual type of our value
            IntWritable value = (IntWritable) values.next();
            frequencyForCountry += value.get();

        } 
        output.collect(key, new Intwritable (frequencyForCountry)); 
    }

}
