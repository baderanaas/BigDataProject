package seasonalanalysis;

import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class SeasonalAnalysisMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final IntWritable ONE = new IntWritable(1);
    private final Text outputKey = new Text();

    // Date formatter for parsing the date strings
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

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
                String inspectionDate = fields[12].trim(); // inspection_date
                String result = fields[13].trim(); // result

                if (!inspectionDate.isEmpty() && !result.isEmpty()) {
                    try {
                        // Parse the date
                        LocalDate date = LocalDate.parse(inspectionDate, dateFormatter);

                        // Extract month and year
                        int month = date.getMonthValue();
                        int year = date.getYear();
                        String monthName = date.getMonth().toString();

                        // Create various keys for analysis

                        // 1. Monthly totals (all inspections by month)
                        outputKey.set("MONTH_TOTAL|" + String.format("%02d", month) + "_" + monthName);
                        context.write(outputKey, ONE);

                        // 2. Yearly totals (all inspections by year)
                        outputKey.set("YEAR_TOTAL|" + year);
                        context.write(outputKey, ONE);

                        // 3. Year-Month totals (inspections by specific year-month)
                        outputKey.set("YEAR_MONTH|" + year + "_" + String.format("%02d", month) + "_" + monthName);
                        context.write(outputKey, ONE);

                        // 4. Results by month (failures, passes, etc. by month)
                        outputKey.set("MONTH_RESULT|" + String.format("%02d", month) + "_" + monthName + "|" + result);
                        context.write(outputKey, ONE);

                        // 5. Results by year (failures, passes, etc. by year)
                        outputKey.set("YEAR_RESULT|" + year + "|" + result);
                        context.write(outputKey, ONE);

                        // 6. Results by year-month combination
                        outputKey.set("YEAR_MONTH_RESULT|" + year + "_" + String.format("%02d", month) + "_" + monthName
                                + "|" + result);
                        context.write(outputKey, ONE);

                        // 7. Seasonal analysis (group by quarters)
                        String season = getSeason(month);
                        outputKey.set("SEASON_TOTAL|" + season);
                        context.write(outputKey, ONE);

                        outputKey.set("SEASON_RESULT|" + season + "|" + result);
                        context.write(outputKey, ONE);

                        // 8. Day of week analysis (if needed)
                        String dayOfWeek = date.getDayOfWeek().toString();
                        outputKey.set("DAYOFWEEK_TOTAL|" + dayOfWeek);
                        context.write(outputKey, ONE);

                        outputKey.set("DAYOFWEEK_RESULT|" + dayOfWeek + "|" + result);
                        context.write(outputKey, ONE);

                    } catch (DateTimeParseException e) {
                        // Handle date parsing errors
                        context.getCounter("Date Errors", "Parse Failures").increment(1);
                    }
                }
            }
        } catch (CsvValidationException e) {
            // Handle CSV parsing errors - log but continue processing
            context.getCounter("CSV Errors", "Parse Failures").increment(1);
        }
    }

    /**
     * Determine season based on month
     */
    private String getSeason(int month) {
        switch (month) {
            case 12:
            case 1:
            case 2:
                return "Winter";
            case 3:
            case 4:
            case 5:
                return "Spring";
            case 6:
            case 7:
            case 8:
                return "Summer";
            case 9:
            case 10:
            case 11:
                return "Fall";
            default:
                return "Unknown";
        }
    }
}