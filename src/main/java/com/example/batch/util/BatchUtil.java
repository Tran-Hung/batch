package com.example.batch.util;

import com.example.batch.common.CommonKeys;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import javax.validation.ConstraintViolation;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Created by Hai NV on 8/9/2017.
 */
@Slf4j
public class BatchUtil {
    public static String buildErrorMessageDetail(Set<ConstraintViolation<Object>> contentConstraintViolations) {
        StringBuilder errorMessage = new StringBuilder();
        try {
            for (ConstraintViolation<Object> constraintViolation : contentConstraintViolations) {

                errorMessage.append(constraintViolation.getPropertyPath());
                errorMessage.append(" have error: ");
                errorMessage.append(constraintViolation.getMessage());
                errorMessage.append(" | ");
            }
        } catch (Exception ex) {
            log.error("", ex);
        }
        return errorMessage.toString();
    }

    public static String getCurrentFolder(String input) throws IOException {
        StringBuilder result = new StringBuilder();
        result.append(input);
        result.append(File.separator);
        DateFormat dateFormat = new SimpleDateFormat(CommonKeys.BATCH_FORMAT_DATE);
        result.append(dateFormat.format(new Date()));
        result.append(File.separator);
        File inputFolder = new File(result.toString());
        if (!inputFolder.exists()) {
            Files.createDirectories(Paths.get(result.toString()));
        }
        return result.toString();
    }


    /**
     * Check folder is empty or not
     *
     * @param path
     * @return
     * @throws IOException
     */
    public static boolean isEmpty(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (Stream<Path> entries = Files.list(path)) {
                return !entries.findFirst().isPresent();
            }
        }

        return false;
    }

//    /**
//     * Housekeeping file by folder and the date want to keep (all file created before the dateToKeep will be move to backupDir)
//     *
//     * @param backupDir
//     * @param dateToKeep
//     * @param emailFolder
//     * @return number file processed
//     */
//    public static int houseKeepingByFolder(String backupDir, Date dateToKeep, File emailFolder, String excludeFileExtension) {
//        int numberProcessedFile = 0;
//        Iterator<File> fileToHouseKeeping = FileUtils.iterateFiles(emailFolder, new AgeFileFilter(dateToKeep), TrueFileFilter.TRUE);
//        while (fileToHouseKeeping.hasNext()) {
//            try {
//                File file = fileToHouseKeeping.next();
//                if (!file.exists() || (StringUtils.isNotEmpty(excludeFileExtension) && StringUtils.equalsIgnoreCase(FilenameUtils.getExtension(file.getName()), excludeFileExtension))) {
//                    continue;
//                }
//
//                Path backup = Paths.get(backupDir + file.getAbsolutePath());
//                backup.getParent().toFile().mkdirs();
//
//                Files.move(file.toPath(), backup);
//                log.info("\nFile: [{}] was moved to: [{}]", file.getPath(), backup);
//                Path folder = file.toPath().getParent();
//                if (isEmpty(folder)) {
//                    if(!folder.toFile().delete()){
//                        throw new IOException("Can't delete file!");
//                    }
//                }
//                numberProcessedFile++;
//            } catch (IOException e) {
//                log.error("", e);
//                continue;
//            }
//        }
//        return numberProcessedFile;
//    }
}
