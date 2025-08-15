package com.example.load.spcc.bo_batch_load_spcc.utils;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.List;

@Slf4j
public class BatchUtils {


    public static String getArgumentValue(ApplicationArguments args, String name) {
        List<String> values = args.getOptionValues(name);
        if (values != null && !values.isEmpty() && !isNullOrEmpty(values.get(0))) {
            return values.get(0);
        }
        log.error("Se espera un valor en el parametro --{}: ", name);
        System.exit(1);
        return null;
    }

    public static LocalDate getArgumentValueAsLocalDate(ApplicationArguments args, String name, String format) {
        String dateStr = getArgumentValue(args, name);
        if (dateStr != null) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
                return LocalDate.parse(dateStr, formatter);
            } catch (DateTimeParseException e) {
                log.error("Formato de fecha incorrecto para {}: {}. Se espera formato: {}", name, dateStr, format);
                System.exit(1);
            }
        }
        return null;
    }

    public static boolean isNullOrEmpty(String str) {
        return str == null || str.isEmpty();
    }

}

