package tech.artcoded.csvtottl.utils;

import com.opencsv.CSVReader;
import lombok.SneakyThrows;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public interface CSVReaderUtils {

    @SneakyThrows
    static List<String[]> readList(InputStream is){
        try (CSVReader reader = new CSVReader(new InputStreamReader(is))) {
           return reader.readAll();
        }
    }
    @SneakyThrows
    static List<Map<String,String>> readMap(InputStream is){
        try (CSVReader reader = new CSVReader(new InputStreamReader(is))) {
            List<String[]> lines = reader.readAll();
            Supplier<Exception> exc = () ->new RuntimeException("At least two lines must be in the csv, first one has to be the header");
            if(lines.size() < 2) {
                throw exc.get();
            }
            String[] titles = lines.stream().findFirst().orElseThrow(exc);
            return lines.stream().skip(1).map(strings -> IntStream.range(0, titles.length)
                    .mapToObj(index -> Map.entry(titles[index].replaceAll("\"",""),strings[index].replaceAll("\"","")))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
                    .collect(Collectors.toUnmodifiableList());
        }
    }
}
