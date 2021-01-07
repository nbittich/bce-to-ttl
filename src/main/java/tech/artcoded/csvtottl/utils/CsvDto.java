package tech.artcoded.csvtottl.utils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CsvDto {
    String[] titles;
    List<Map<String,String>> csv;
}
