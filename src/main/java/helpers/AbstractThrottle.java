package helpers;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * AbstractThrottle
 */
public interface AbstractThrottle {

  /**
   * asyncAcquire
   * 
   * @param permits
   */
  ListenableFuture<?> asyncAcquire(Number permits);
  
}
