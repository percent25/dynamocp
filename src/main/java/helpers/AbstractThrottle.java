package helpers;

/**
 * AbstractThrottle
 */
public interface AbstractThrottle {

  /**
   * acquire
   * 
   * @param permits
   */
  void acquire(Number permits);
  
}
