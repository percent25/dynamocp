package helpers;

import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.slf4j.LoggerFactory;

/**
 * LogHelper
 */
public class LogHelper {
  private final Object self;

  /**
   * ctor
   * 
   * @param self
   */
  public LogHelper(Object self) {
    this.self = self;
  }

  /**
   * log
   * 
   * @param args
   */
  public void log(Object... args) {
    List<String> parts = Lists.newArrayList();
    // parts.add(new Date().toString());
    // parts.add(String.format("[%s]", Thread.currentThread().getName()));
    parts.add(self.getClass().getSimpleName());
    for (Object arg : args)
      parts.add("" + arg);
    System.err.println(String.join(" ", parts));
  }

  public void debug(Object... args) {
    LoggerFactory.getLogger(self.getClass()).debug(Strings.repeat("{} ", args.length), args);
  }

}