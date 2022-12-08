package utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.stream.Stream;

public class FileManager {

    private static String readLine(String filePath, int line) {
        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            return lines.skip(line-1).findFirst().orElse(null);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static int getNumberOfLines(String filePath) {
        try (Stream<String> lines = Files.lines(Paths.get(filePath))) {
            return (int)lines.count();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static LinkedList<String> readLastNLines(String filePath, int lines) {
        LinkedList<String> entries = new LinkedList<>();
        int numberOfLines = getNumberOfLines(filePath);
        for (int i = 0; i < lines; i++) {
            entries.addFirst(readLine(filePath, numberOfLines - i));
        }
        return entries;
    }

    public static void addLine(String filePath, String line) {
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(filePath, true));
            bw.write(line);
            bw.newLine();
            bw.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    /*
    public static void main(String[] args) {
        //System.out.println(readLine(".\\src\\common\\test.txt", 2));
        //deleteFromLine(".\\src\\common\\test.txt", 4);
        //addLine(".\\src\\common\\test.txt", "oDiogoéGay");
        //System.out.println(getFileSize(".\\src\\common\\test.txt"));;
        //System.out.println(readLastNLines("ola.txt", 4));
    }*/
}