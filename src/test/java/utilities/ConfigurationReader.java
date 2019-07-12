package utilities;


import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

public class ConfigurationReader {

    private static Properties properties;

    public static String getProperty(String key){
        String path = "configuration.properties";

        try {
            FileInputStream stream = new FileInputStream(path);
            properties = new Properties();
            properties.load(stream);
            stream.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return properties.getProperty(key);
    }
}

