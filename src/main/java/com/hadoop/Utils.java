package com.hadoop;

import java.util.ArrayList;
import java.util.List;

public class Utils {

    public static String[] parseCSVLine(String line) {
        List<String> values = new ArrayList<>();
        StringBuilder value = new StringBuilder();
        boolean inQuotes = false;

        char[] chars = line.toCharArray();
        for (int i = 0; i < chars.length; i++) {
            char ch = chars[i];

            if (ch == '"') {
                // Toggle inQuotes on double quote, but handle consecutive double quotes
                if (inQuotes && i + 1 < chars.length && chars[i + 1] == '"') {
                    value.append(ch); // Add a single quote to the value
                    i++; // Skip the next quote
                } else {
                    inQuotes = !inQuotes;
                }
            } else if (ch == ',' && !inQuotes) {
                values.add(value.toString());
                value = new StringBuilder();
            } else {
                value.append(ch);
            }
        }
        values.add(value.toString()); // Add the last field

        return values.toArray(new String[0]);
    }


    /**
     * @param point Point in form of <code> POINT (-93.10564100000002 41.320746)</code>
     * @return Array of longitude and latitude
     */
    public static Double[] extractLocation(String point) { // POINT (-93.10564100000002 41.320746)
        point = point.replaceAll("\\(","").replaceAll("\\)", ""); // POINT -93.10564100000002 41.320746
        String[] data = point.split(" ");

        double longitude = Double.parseDouble(data[1]);
        double latitude = Double.parseDouble(data[2]);

        return new Double[] {longitude, latitude};
    }
}
