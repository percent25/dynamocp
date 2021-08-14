package percent25.awscat;

/**
 * InputPluginProvider
 */
public interface InputPluginProvider {

  /**
   * name
   * 
   * @return
   */
  String name();

  /**
   * help
   * 
   * @return
   */
  String help();

  /**
   * canActivate
   * 
   * @param address e.g., "dynamo:MyTable,c=1"
   * @return
   */
  boolean canActivate(String address);

  /**
   * activate
   * 
   * @param address e.g., "dynamo:MyTable,c=1"
   * @return
   * @throws Exception
   */
  InputPlugin activate(String address) throws Exception;

}
