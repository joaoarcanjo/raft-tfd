package utils;

import java.util.Random;

public class Utils {
    public static int randomizedTimer(int min, int max) {
        Random random = new Random();
        return random.nextInt(max - min) + min;
    }
}
