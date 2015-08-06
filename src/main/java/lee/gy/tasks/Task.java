package lee.gy.tasks;

import lee.gy.ExampleMain;
import org.apache.reef.tang.annotations.Parameter;

/**
 * Abstarct Class for dummy Mapper & Reducer
 */
public abstract class Task {

  protected int arriveTime, duration;

  public Task(int arriveTime, int duration) {
    this.arriveTime = arriveTime;
    this.duration = duration;
  }
}
