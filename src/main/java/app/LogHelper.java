package app;

import java.util.*;

import com.google.common.base.*;

/**
 * LogHelper
 */
public class LogHelper {
  private final Object object;

  public LogHelper(Object object) {
    this.object = object;
  }

  public void log(Object... args) {
    List<String> parts = new ArrayList<>();
//    parts.add(new Date());
//    parts.add(object);
    for (Object arg : args)
      parts.add(MoreObjects.firstNonNull(arg, "null").toString());
    System.err.println(String.join(" ", parts));
  }
}