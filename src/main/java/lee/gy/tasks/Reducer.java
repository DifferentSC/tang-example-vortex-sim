package lee.gy.tasks;

import lee.gy.ExampleMain;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A dummy Reducer class
 */
public class Reducer extends Task {

  @Inject
  public Reducer(
      @Parameter(ExampleMain.ArriveTime.class) int arriveTime,
      @Parameter(ExampleMain.Duration.class) int duration) {
    super(arriveTime, duration);
    System.out.println(String.format("Reducer created with %d arriving time and %d duration",
        this.arriveTime, this.duration));
  }

}
