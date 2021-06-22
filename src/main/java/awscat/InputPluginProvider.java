package awscat;

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
   * @param address e.g., "dynamo:MyTable,limit=1000"
   * @return
   */
  boolean canActivate(String address);

  /**
   * activate
   * 
   * @param address e.g., "dynamo:MyTable,limit=1000"
   * @return
   * @throws Exception
   */
  InputPlugin activate(String address) throws Exception;

}
