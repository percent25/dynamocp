package io.github.awscat;

import org.springframework.util.ClassUtils;

/**
 * InputPluginProvider
 */
public interface InputPluginProvider {

  /**
   * name
   * 
   * @return
   */
  default String name() {
    return ClassUtils.getShortName(getClass());
  }

  /**
   * help
   * 
   * @return
   */
  String help();

  /**
   * canActivate
   * 
   * @param arg e.g., "dynamo:MyTable,limit=1000"
   * @return
   */
  boolean canActivate(String arg);

  /**
   * activate
   * 
   * @param arg e.g., "dynamo:MyTable,limit=1000"
   * @return
   * @throws Exception
   */
  InputPlugin activate(String arg) throws Exception;

}
