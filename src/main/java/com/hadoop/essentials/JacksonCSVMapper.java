package com.hadoop.essentials;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


public class JacksonCSVMapper {


    public static void main(String[] args) throws IOException {

        CsvMapper  csvMapper = new CsvMapper();
        CsvSchema schema = CsvSchema.emptySchema().withHeader();

        ObjectReader objectReader = csvMapper.readerFor(OnlineCourse.class).with(schema);
        List<OnlineCourse> courses = new ArrayList<>();

        try (Reader reader =new FileReader("data/test.csv")) {
            MappingIterator<OnlineCourse> iterator = objectReader.readValues(reader);
            while (iterator.hasNext()) {
                OnlineCourse onlineCourse = iterator.next();
                courses.add(onlineCourse);
                System.out.println(onlineCourse);
            }
        }

    }
}




