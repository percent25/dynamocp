package helpers;

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

  // ctlplane
  public void debug(Object... args) {
    // System.out.println(self.getClass().getSimpleName()+Lists.newArrayList(args));
    LoggerFactory.getLogger(self.getClass()).debug(Strings.repeat("{} ", args.length), args);
  }

  // dataplane
  public void trace(Object... args) {
    // System.out.println(self.getClass().getSimpleName()+Lists.newArrayList(args));
    LoggerFactory.getLogger(self.getClass()).trace(Strings.repeat("{} ", args.length), args);
  }

}