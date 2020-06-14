package com.lrh.flume.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author lironghui
 * @version 1.0
 * @date 2019/12/14 13:50
 */
public class FileUtil {
    private static final Logger logger = LoggerFactory.getLogger(FileUtil.class);
    private static String LINE_SEPARATOR = System.getProperty("line.separator");
    private static final StandardOpenOption[] APPEND_OPTION = {StandardOpenOption.CREATE, StandardOpenOption.APPEND};
    private static final StandardOpenOption[] OPTION = {StandardOpenOption.CREATE};
    private static final boolean DEFAUTL_APPEND = false;
    private static final boolean DEFAUTL_REPLACE_EXISTS = true;
    private static final List EMPTY_LIST = new ArrayList();


    public static boolean exist(String path) {
        return Files.exists(Paths.get(path), new LinkOption[]{LinkOption.NOFOLLOW_LINKS});
    }

    public static boolean isDirectory(String path) {
        return Files.isDirectory(Paths.get(path));
    }

    public static void delete(String path) throws IOException {
        Files.deleteIfExists(Paths.get(path));
    }

    public static void createParentDirectory(String path) throws IOException {
        Files.createDirectory(Paths.get(path).getParent());
    }

    public static void createDirectory(String path) throws IOException {
        Files.createDirectory(Paths.get(path));
    }

    public static void copy(String srcDir, String destDir) throws IOException, FileAlreadyExistsException {
        copy(srcDir, destDir, DEFAUTL_REPLACE_EXISTS);
    }

    public static void copy(String srcDir, String destDir, boolean replaceExisting) throws IOException {
        checkFileRequirements(srcDir, destDir);
        if (replaceExisting) {
            Files.copy(Paths.get(srcDir), Paths.get(destDir), StandardCopyOption.REPLACE_EXISTING);
        } else {
            Files.copy(Paths.get(srcDir), Paths.get(destDir));
        }
    }


    public static void createFile(String path) throws IOException {
        Files.createFile(Paths.get(path));
    }

    public static void move(String srcDir, String destDir) throws IOException {
        move(srcDir, destDir, DEFAUTL_REPLACE_EXISTS);
    }

    public static void move(String srcDir, String destDir, boolean replaceExisting) throws IOException {
        checkFileRequirements(srcDir, destDir);
        if (replaceExisting) {
            Files.move(Paths.get(srcDir), Paths.get(destDir), StandardCopyOption.REPLACE_EXISTING);
        } else {
            Files.move(Paths.get(srcDir), Paths.get(destDir));
        }
    }

    public static List<String> readAllLines(String path) throws IOException {
        return readAllLines(path, Charset.defaultCharset());
    }

    public static List<String> readAllLines(String path, Charset sc) throws IOException {
        return Files.readAllLines(Paths.get(path), sc);
    }

    public static byte[] readAllBytes(String path) throws IOException {
        return Files.readAllBytes(Paths.get(path));
    }

    public static String readToString(String path) throws IOException {
        return readToString(path, Charset.defaultCharset());
    }


    public static String readToString(String path, Charset sc) throws IOException {
        return readToString(Paths.get(path), sc);
    }
    public static String readToString(Path path) throws IOException {
        return readToString(path, Charset.defaultCharset());
    }
    public static String readToString(Path path, Charset sc) throws IOException {
        return new String(Files.readAllBytes(path), sc);
    }

    public static void write(String path, String conetent) throws IOException {
        write(path, conetent, Charset.defaultCharset(), DEFAUTL_APPEND);
    }

    public static void write(String path, String conetent, boolean append) throws IOException {
        write(path, conetent, Charset.defaultCharset(), append);
    }

    public static void write(String path, String conetent, Charset cs) throws IOException {
        write(path, conetent.getBytes(cs), DEFAUTL_APPEND);
    }

    public static void write(String path, String conetent, Charset cs, boolean append) throws IOException {
        write(path, conetent.getBytes(cs), append);
    }

    public static void writeLines(String path, List<String> lines) throws IOException {
        writeLines(path, lines, Charset.defaultCharset(), DEFAUTL_APPEND);
    }

    public static void writeLines(String path, List<String> lines, boolean append) throws IOException {
        writeLines(path, lines, Charset.defaultCharset(), append);
    }

    public static void writeLines(String path, List<String> lines, Charset cs) throws IOException {
        writeLines(path, lines, cs, DEFAUTL_APPEND);
    }

    public static void writeLines(String path, List<String> lines, Charset cs, boolean append) throws IOException {
        if (lines == null || lines.size() == 0) {
            return;
        }
        byte[] separatorBytes = LINE_SEPARATOR.getBytes(cs);
        try (OutputStream out = Files.newOutputStream(Paths.get(path), getWriteOptions(append))) {
            for (String line : lines) {
                if (line != null) {
                    out.write(line.getBytes(cs));
                }
                out.write(separatorBytes);
            }
        }
    }

    private static StandardOpenOption[] getWriteOptions(boolean append) {
        if (append) {
            return APPEND_OPTION;
        } else {
            return OPTION;
        }
    }

    public static void write(String path, byte[] bytes) throws IOException {
        write(path, bytes, DEFAUTL_APPEND);
    }

    public static void write(String path, byte[] bytes, boolean append) throws IOException {
        if (append) {
            Files.write(Paths.get(path), bytes, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } else {
            Files.write(Paths.get(path), bytes, StandardOpenOption.CREATE);
        }
    }

    public static List<File> listFiles(String path) throws IOException {
        return listPath(path).stream().map(Path::toFile).collect(Collectors.toList());
    }

    public static List<Path> listPath(String path) throws IOException {
        return listPath(path, (e) -> !Files.isDirectory(e));
    }

    public static List<Path> listPath(String path, Predicate<Path> predicate) throws IOException {
        if (!isDirectory(path)) {
            return EMPTY_LIST;
        }
        List<Path> result = new ArrayList<>();
        Files.walkFileTree(Paths.get(path), new FilterFileVisitor(predicate, result));
        return result;
    }

    /**
     * checks requirements for file copy
     *
     * @param src  the source file
     * @param dest the destination
     * @throws FileNotFoundException if the destination does not exist
     */
    private static void checkFileRequirements(String src, String dest) throws IOException {
        if (src == null) {
            throw new NullPointerException("Source must not be null");
        }
        if (dest == null) {
            throw new NullPointerException("Destination must not be null");
        }
        if (!exist(src)) {
            throw new FileNotFoundException("Source '" + src + "' does not exist");
        }
        if (!exist(dest)) {
            if (isDirectory(dest)) {
                createDirectory(dest);
            } else {
                createParentDirectory(dest);
            }
        }
    }

    static class FilterFileVisitor extends SimpleFileVisitor<Path> {
        private Predicate<Path> predicate;
        private List<Path> result;

        public FilterFileVisitor(Predicate<Path> predicate, List<Path> result) {
            this.predicate = predicate;
            this.result = result;
        }

        public Predicate<Path> getPredicate() {
            return predicate;
        }

        public void setPredicate(Predicate<Path> predicate) {
            this.predicate = predicate;
        }

        public List<Path> getResult() {
            return result;
        }

        public void setResult(List<Path> result) {
            this.result = result;
        }

        public FilterFileVisitor(Predicate predicate) {
            this.predicate = predicate;
        }

        @Override
        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
            return super.preVisitDirectory(dir, attrs);
        }

        @Override
        public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
            System.out.println(file.getFileName());
            if (predicate.test(file)) {

                result.add(file);
            }
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
            return FileVisitResult.CONTINUE;
        }

        @Override
        public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
            return super.postVisitDirectory(dir, exc);
        }
    }

    public static void main(String[] args) {
        String path = "E:\\迅雷下载\\Win10x64.GHO";
        String path2 = "E:\\迅雷下载\\nio\\Win10x64.GHO2";
        String path3 = "E:\\迅雷下载";


        try {
            listPath(path3, new Predicate<Path>() {
                @Override
                public boolean test(Path path) {
                    return path.getFileName().toString().equals("11.txt");

                }
            }).stream().forEach(e -> System.out.println(e.toAbsolutePath()));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
