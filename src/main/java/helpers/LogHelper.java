package helpers;

import java.util.*;

import com.google.common.base.*;
import com.google.common.collect.*;

import org.slf4j.*;

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

  public String str(Object... args) {
    List<String> parts = Lists.newArrayList();
    // parts.add(new Date().toString());
    // parts.add(String.format("[%s]", Thread.currentThread().getName()));
    // parts.add(ClassUtils.getShortName(self.getClass()));
    for (Object arg : args)
      parts.add("" + arg);
    return String.join(" ", parts);
  }
  
  // public void stdout(Object... args) {
  //   System.out.println(str(args));
  // }

  // public void stderr(Object... args) {
  //   System.err.println(str(args));
  // }

  public void debug(Object... args) {
    LoggerFactory.getLogger(self.getClass()).debug(Strings.repeat("{} ", args.length), args);
  }

  public void trace(Object... args) {
    LoggerFactory.getLogger(self.getClass()).trace(Strings.repeat("{} ", args.length), args);
  }

}