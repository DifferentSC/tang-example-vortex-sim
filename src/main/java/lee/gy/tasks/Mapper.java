package lee.gy.tasks;

import lee.gy.ExampleMain;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

/**
 * A dummy Mapper class
 */
public class Mapper extends Task {

  @Inject
  public Mapper(
      @Parameter(ExampleMain.ArriveTime.class) int arriveTime,
      @Parameter(ExampleMain.Duration.class) int duration) {
    super(arriveTime, duration);
    System.out.println(String.format("Mapper created with %d arriving time and %d duration",
        this.arriveTime, this.duration));
  }
}
