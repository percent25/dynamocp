package helpers;

import java.util.Date;
import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;

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
    parts.add(new Date().toString());
    // parts.add(String.format("[%s]", Thread.currentThread().getName()));
    // parts.add(ClassUtils.getShortName(self.getClass()));
    for (Object arg : args)
      parts.add("" + arg);
    System.err.println(String.join(" ", parts));
  }

  public void debug(Object... args) {

    // Object newArgs = new Object[args.length+1];
    // System.arraycopy(args, 0, newArgs, 1, args.length);
    // newArgs[0] = extraVar; 
    // String.format(format, extraVar, args);    

    // static <T> T[] append(T[] arr, T lastElement) {
    //   final int N = arr.length;
    //   arr = java.util.Arrays.copyOf(arr, N+1);
    //   arr[N] = lastElement;
    //   return arr;
    // }
    LoggerFactory.getLogger(self.getClass()).debug(Strings.repeat("{} ", args.length), args);
  }

}