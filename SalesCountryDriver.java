package sales;

import org.apache.hadoop.fs.Path;

public class SalesCountryDriver{ 
    
    public static void main(String[] args) { 
    
        JobClient my_client = new JobClient();

        // Create a configuration object for the job 
        JobConf job conf = new JobConf (SalesCountryDriver.class);
        // Set a name of the Job
        job_conf.setJobName("SalePerCountry");


        // Specify data type of output key and value
        job_conf.setOutputKeyClass (Text.class);
        job_conf.setOutputValueClass (IntWritable.class);

        // Specify names of Mapper and Reducer Class 
        job_conf.setMapperclass (sales.SalesMapper.class); 
        job_conf.setReducerClass(sales.SalesCountryReducer.class):

        // Specify formats of the data type of Input and output 
        job_conf.setInputFormat (TextInputFormat.class);
        job_conf.setOutputFormat (TextOutputFormat.class);


        // Set input and output directories using command line arguments. 
        // arg[0] = name of input directory on HOFS, and arg[1]= name of output directory to be created to store the output
        FileInputFormat.setInputPaths (job conf, new Path(args[e]));
        FileOutputFormat.setOutputPath(job conf, new Path(args[1]));

        my_client.setConf(job_conf);

        try {
            // Run the job
            JobClient.runJob(jab_conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
