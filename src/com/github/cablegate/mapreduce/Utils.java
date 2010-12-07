package com.github.cablegate.mapreduce;

import java.util.ArrayList;

public class Utils {

    public static String[] extractCategories(String text) {
        ArrayList<String> categories = new ArrayList<String>();
        int pos = text.indexOf("[[Category:");
        while (pos != -1) {
            text = text.substring(pos + 11);
            int end = text.indexOf("]]");
            if (end == -1) {
                break;
            }
            categories.add(normalizeCategory(text.substring(0, end)));
            pos = text.indexOf("[[Category:");
        }
        if (categories.size() == 0) {
            return new String[0];
        }
        String result[] = new String[categories.size()];
        return categories.toArray(result);
    }

    private final static String normalizeCategory(String category) {
        String fragments[] = category.trim().toUpperCase().split("\\s");
        StringBuilder sb = new StringBuilder(category.length() + 1);
        for (String fragment: fragments) {
            if (fragment.length() == 0) {
                continue;
            }
            if (sb.length() > 0) {
                sb.append(' ');
            }
            sb.append(fragment);
        }
        return sb.toString();
    }

}
