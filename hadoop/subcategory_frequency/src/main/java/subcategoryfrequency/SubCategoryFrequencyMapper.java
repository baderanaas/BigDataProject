package subcategoryfrequency;

import java.io.IOException;
import java.io.StringReader;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class SubCategoryFrequencyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private final Text subcategoryKey = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Skip header
        if (key.get() == 0 && line.startsWith("inspection_id")) {
            return;
        }

        try (CSVReader csvReader = new CSVReader(new StringReader(line))) {
            // Read the single line
            String[] fields = csvReader.readNext();

            // Make sure we have enough fields to access the inspection_subcategory (index 8)
            if (fields != null && fields.length > 8) {
                String subcategory = fields[10].trim();
                if (!subcategory.isEmpty()) {
                    subcategoryKey.set(subcategory);
                    context.write(subcategoryKey, ONE);
                }
            }
        } catch (CsvValidationException e) {
            // Handle CSV parsing errors - log but continue processing
            context.getCounter("CSV Errors", "Parse Failures").increment(1);
        }
    }
}