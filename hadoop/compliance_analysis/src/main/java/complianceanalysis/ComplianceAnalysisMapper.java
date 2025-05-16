package complianceanalysis;

import java.io.IOException;
import java.io.StringReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;

public class ComplianceAnalysisMapper extends Mapper<LongWritable, Text, Text, Text> {
    private final Text outputKey = new Text();
    private final Text outputValue = new Text();
    private final DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();

        if (key.get() == 0 && (line.startsWith(",inspection_id") || line.startsWith("inspection_id"))) {
            return; // Skip header
        }

        try (CSVReader csvReader = new CSVReader(new StringReader(line))) {
            String[] fields = csvReader.readNext();

            if (fields != null && fields.length > 13) {
                String category = fields[9].trim();
                String subcategory = fields[10].trim();
                String inspector = fields[11] != null ? fields[11].trim() : "N/A";
                String inspectionDate = fields[12].trim();
                String result = fields[13].trim();

                if (!category.isEmpty() && !result.isEmpty()) {
                    boolean passed = isPassedResult(result);
                    String passedStr = passed ? "1" : "0";
                    String valueStr = passedStr + ",1";

                    context.write(new Text("OVERALL"), new Text(valueStr));
                    context.write(new Text("CATEGORY|" + category), new Text(valueStr));

                    if (!subcategory.isEmpty()) {
                        context.write(new Text("SUBCATEGORY|" + subcategory), new Text(valueStr));
                        context.write(new Text("CATEGORY_SUBCATEGORY|" + category + "|" + subcategory),
                                new Text(valueStr));
                    }

                    context.write(new Text("INSPECTOR|" + inspector), new Text(valueStr));

                    if (!inspectionDate.isEmpty()) {
                        try {
                            LocalDate date = LocalDate.parse(inspectionDate, dateFormatter);
                            String monthKey = String.format("%02d_%s", date.getMonthValue(),
                                    date.getMonth().toString());

                            context.write(new Text("MONTH|" + monthKey), new Text(valueStr));
                            context.write(new Text("YEAR|" + date.getYear()), new Text(valueStr));
                            context.write(new Text("YEAR_MONTH|" + date.getYear() + "_" + monthKey),
                                    new Text(valueStr));
                        } catch (DateTimeParseException e) {
                            context.getCounter("Date Errors", "Parse Failures").increment(1);
                        }
                    }
                } else {
                    context.getCounter("Data Errors", "Missing Category or Result").increment(1);
                }
            } else {
                context.getCounter("Data Errors", "Insufficient Fields").increment(1);
            }
        } catch (CsvValidationException e) {
            context.getCounter("CSV Errors", "Parse Failures").increment(1);
        }
    }

    private boolean isPassedResult(String result) {
        if (result == null)
            return false;

        String normalized = result.trim().toUpperCase();
        switch (normalized) {
            case "INSPECTION PASSED":
            case "COMPLY":
            case "INSPECTION COMPLIED":
            case "ROUTINE INSPECTION PASSED":
            case "RESOLVED":
            case "COMPLAINT INVALID":
            case "EXEMPT":
            case "RESUME OPERATIONS":
                return true;
            default:
                return false;
        }
    }
}
