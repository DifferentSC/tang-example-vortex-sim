package lee.gy;

import lee.gy.tasks.Mapper;
import lee.gy.tasks.Reducer;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.JavaConfigurationBuilder;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.tang.formats.AvroConfigurationSerializer;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Main class for the example
 */
public class ExampleMain {

  @NamedParameter(doc="Arrive time for Task", short_name="arr_time")
  public static class ArriveTime implements Name<Integer> {
  }
  @NamedParameter(doc="Duration for Task", short_name="duration")
  public static class Duration implements Name<Integer> {
  }

  public static void main(String args[]) throws InjectionException {

    /* Method 1: Creating dummy Mapper & Reducers with plain text file  */
    System.out.println("Using plain configuration file");
    BufferedReader in = null;
    try {
      in = new BufferedReader(
          new FileReader(ExampleMain.class.getResource("/plain_configuration.txt").getPath()));
      // Read configuration of mappers
      // Read the number of mappers
      String line = in.readLine();
      int numMappers = Integer.parseInt(line);
      List<Mapper> mapperList = new ArrayList<Mapper>();
      // Read each mapper configuration
      for (int i = 0; i < numMappers; i++) {
        // Read configuration file
        String[] splitLine = in.readLine().split(" ");
        String arriveTime = splitLine[0];
        String duration = splitLine[1];
        // Make a configuration
        JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
        // NamedParameter's value should be string!
        cb.bindNamedParameter(ArriveTime.class, arriveTime);
        cb.bindNamedParameter(Duration.class, duration);
        // Build Configuration
        Configuration conf = cb.build();
        // Get an object with injector
        Injector injector = Tang.Factory.getTang().newInjector(conf);
        Mapper mapper = injector.getInstance(Mapper.class);
        mapperList.add(mapper);
      }

      // Read configuration of reducers
      // Read the number of reducers
      line = in.readLine();
      int numReducers = Integer.parseInt(line);
      List<Reducer> reducerList = new ArrayList<Reducer>();
      // Read each mapper configuration
      for (int i = 0; i < numReducers; i++) {
        // Read configuration file
        String[] splitLine = in.readLine().split(" ");
        String arriveTime = splitLine[0];
        String duration = splitLine[1];
        // Make a configuration
        JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
        // NamedParameter's value should be string!
        cb.bindNamedParameter(ArriveTime.class, arriveTime);
        cb.bindNamedParameter(Duration.class, duration);
        // Build Configuration
        Configuration conf = cb.build();
        // Get an object with injector
        Injector injector = Tang.Factory.getTang().newInjector(conf);
        Reducer reducer = injector.getInstance(Reducer.class);
        reducerList.add(reducer);
      }
      in.close();
    } catch (FileNotFoundException e) {
      System.out.println("File not found!");
    } catch (IOException e) {
      System.out.println("IOException occured!");
    }

    /* Method 2: Creating dummy Mapper & Reducers with avro configuration file  */
    System.out.println("Using avro configuration file");
    try {
      in = new BufferedReader(
          new FileReader(ExampleMain.class.getResource("/avro_configuration.txt").getPath()));
      // Read configuration of mappers
      // Read the number of mappers
      String line = in.readLine();
      int numMappers = Integer.parseInt(line);
      List<Mapper> mapperList = new ArrayList<Mapper>();
      // Read each mapper configuration
      for (int i = 0; i < numMappers; i++) {
        // Read configuration directly
        String avroConfString = in.readLine();
        Configuration conf = new AvroConfigurationSerializer().fromString(avroConfString);
        // Get an object with injector
        Injector injector = Tang.Factory.getTang().newInjector(conf);
        Mapper mapper = injector.getInstance(Mapper.class);
        mapperList.add(mapper);
      }

      // Read configuration of reducers
      // Read the number of reducers
      line = in.readLine();
      int numReducers = Integer.parseInt(line);
      List<Reducer> reducerList = new ArrayList<Reducer>();
      // Read each mapper configuration
      for (int i = 0; i < numReducers; i++) {
        // Read configuration directly
        String avroConfString = in.readLine();
        Configuration conf = new AvroConfigurationSerializer().fromString(avroConfString);
        // Get an object with injector
        Injector injector = Tang.Factory.getTang().newInjector(conf);
        Reducer reducer = injector.getInstance(Reducer.class);
        reducerList.add(reducer);
      }
      in.close();
    } catch (FileNotFoundException e) {
      System.out.println("File not found!");
    } catch (IOException e) {
      System.out.println("IOException occured!");
    }

  }
}
