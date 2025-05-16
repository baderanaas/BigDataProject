package outcomeanalysis;

import java.io.IOException;
import java.io.StringReader;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class OutcomeAnalysisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private final Text categoryOutcomeKey = new Text();

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        // Skip header
        if (key.get() == 0 && line.startsWith(",inspection_id")) {
            return;
        }

        try (CSVReader csvReader = new CSVReader(new StringReader(line))) {
            // Read the single line
            String[] fields = csvReader.readNext();

            // Make sure we have enough fields
            if (fields != null && fields.length > 13) {
                String category = fields[9].trim();
                String subcategory = fields[10].trim();
                String result = fields[13].trim();

                if (!category.isEmpty() && !subcategory.isEmpty() && !result.isEmpty()) {
                    // Create keys for different levels of analysis

                    // 1. Category + Subcategory + Result
                    String detailedKey = category + "|" + subcategory + "|" + result;
                    categoryOutcomeKey.set(detailedKey);
                    context.write(categoryOutcomeKey, ONE);

                    // 2. Category + Result (for category-level analysis)
                    String categoryKey = category + "|" + result;
                    categoryOutcomeKey.set(categoryKey);
                    context.write(categoryOutcomeKey, ONE);

                    // 3. Subcategory + Result (for subcategory-level analysis)
                    String subcategoryKey = subcategory + "|" + result;
                    categoryOutcomeKey.set(subcategoryKey);
                    context.write(categoryOutcomeKey, ONE);

                    // 4. Total counts for each category and subcategory
                    categoryOutcomeKey.set("CATEGORY_TOTAL|" + category);
                    context.write(categoryOutcomeKey, ONE);

                    categoryOutcomeKey.set("SUBCATEGORY_TOTAL|" + subcategory);
                    context.write(categoryOutcomeKey, ONE);
                }
            }
        } catch (CsvValidationException e) {
            // Handle CSV parsing errors - log but continue processing
            context.getCounter("CSV Errors", "Parse Failures").increment(1);
        }
    }
}