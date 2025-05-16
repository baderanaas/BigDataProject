package complianceanalysis;

import java.io.IOException;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

public class ComplianceAnalysisReducer extends Reducer<Text, Text, Text, Text> {
    private final Text result = new Text();

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        int passed = 0;
        int total = 0;

        for (Text val : values) {
            String[] parts = val.toString().split(",");
            if (parts.length == 2) {
                passed += Integer.parseInt(parts[0]);
                total += Integer.parseInt(parts[1]);
            }
        }

        double complianceRate = total > 0 ? ((double) passed / total) * 100.0 : 0.0;
        result.set(String.format("%d,%d,%.2f%%", passed, total, complianceRate));
        context.write(key, result);
    }
}
