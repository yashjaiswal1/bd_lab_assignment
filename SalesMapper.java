package sales;

import java.io.IOException;

public class SalesMapper extends MapReduceBase implements Mapper <LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
        
    public void map (LongWritable key, Text value, OutputCollector <Text, IntWritable> output, Reporter reporter) throws IOException {

        String valueString= value.toString(); 
        String[] SingleCountryData = valueString.split(".");
        output.collect (new Text(SingleCountryData[7]), one);


    }

}
