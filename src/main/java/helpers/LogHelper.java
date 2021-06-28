package helpers;

import java.util.Date;
import java.util.List;

import com.google.common.base.*;
import com.google.common.collect.*;

// import org.slf4j.*;

/**
 * LogHelper
 */
public class LogHelper {

  public static boolean debug;
  public static boolean trace;
  
  private final Object self;

  /**
   * ctor
   * 
   * @param self
   */
  public LogHelper(Object self) {
    this.self = self;
  }

  private void log(String lvl, Object... args) {
    List<Object> parts = Lists.newArrayList(args);
    parts.add(0, new Date());
    parts.add(1, lvl);
    parts.add(2, String.format("[%s]", self.getClass().getSimpleName()));
    System.out.println(Joiner.on(" ").useForNull("null").join(parts));
  }

  // ctlplane
  public void debug(Object... args) {
    if (debug)
      log("DEBUG", args);
      // System.out.println(self.getClass().getSimpleName()+Joiner.on(" ").useForNull("null").join(args));
    // LoggerFactory.getLogger(self.getClass()).debug(Strings.repeat("{} ", args.length), args);
  }

  // dataplane
  public void trace(Object... args) {
    if (trace)
      log("TRACE", args);
    // System.out.println(self.getClass().getSimpleName()+Joiner.on(" ").useForNull("null").join(args));
    // LoggerFactory.getLogger(self.getClass()).trace(Strings.repeat("{} ", args.length), args);
  }

}