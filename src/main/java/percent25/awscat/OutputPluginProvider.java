package percent25.awscat;

import java.util.function.Supplier;

import org.springframework.util.ClassUtils;

/**
 * OutputPluginProvider
 */
public interface OutputPluginProvider {

  /**
   * name
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
   * @param address
   * @return
   */
  boolean canActivate(String address);
  
  /**
   * activate
   * 
   * @param address
   * @return
   * @throws Exception
   */
  Supplier<OutputPlugin> activate(String address) throws Exception;

  /**
   * mtu
   * 
   * @return
   */
  default int mtu() {
    return -1;
  }

}
