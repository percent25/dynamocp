package helpers;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * AbstractThrottle
 */
public interface AbstractThrottle {

  /**
   * acquire
   * 
   * @param permits
   */
  ListenableFuture<?> asyncAcquire(Number permits);
  
}
