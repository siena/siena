/*
 * @author mandubian <pascal.voitot@mandubian.org>
 */
package siena.gae;

import java.util.Properties;
import siena.PersistenceManager;
import com.googlecode.objectify.cache.CachingDatastoreServiceFactory;
import com.googlecode.objectify.cache.EntityMemcache;

public class GaeCachingPersistenceManagerAsync extends GaePersistenceManagerAsync {

  protected EntityMemcache entityMemcache;

  public GaeCachingPersistenceManagerAsync() {
    super();
  }

  public GaeCachingPersistenceManagerAsync(EntityMemcache em) {
    super();
    entityMemcache = em;
  }

  @Override
  public void init(Properties p) {
    ds = entityMemcache == null ? CachingDatastoreServiceFactory.getAsyncDatastoreService() : CachingDatastoreServiceFactory.getAsyncDatastoreService(entityMemcache);
    props = p;
  }

  @Override
  public void init(Properties p, PersistenceManager syncPm) {
    this.syncPm = syncPm;
    ds = entityMemcache == null ? CachingDatastoreServiceFactory.getAsyncDatastoreService() : CachingDatastoreServiceFactory.getAsyncDatastoreService(entityMemcache);
    props = p;
  }

  @Override
  public PersistenceManager sync() {
    if (syncPm == null) {
      syncPm = new GaeCachingPersistenceManager(entityMemcache);
      syncPm.init(props);
    }
    return syncPm;
  }
}
