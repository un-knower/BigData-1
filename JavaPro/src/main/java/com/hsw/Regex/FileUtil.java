package com.hsw.Regex;

/**
 * Created by hushiwei on 2017/4/20.
 */

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

public class FileUtil {

    public static ArrayList<String> readlines(String path) throws IOException {
        ArrayList<String> lines = new ArrayList<>();
        final LineIterator it = FileUtils.lineIterator(new File(path), "UTF-8");
        try {
            while (it.hasNext()) {
                final String line = it.nextLine();
                lines.add(line);
            }
        } finally {
            it.close();
        }
        return lines;
    }


}
