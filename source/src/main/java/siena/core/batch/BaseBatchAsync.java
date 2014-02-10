package siena.core.batch;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.googlecode.objectify.Key;
import com.googlecode.objectify.util.MakeListResult;
import com.googlecode.objectify.util.ResultCache;
import com.googlecode.objectify.util.ResultProxy;

import siena.core.async.SienaFuture;
import siena.core.async.PersistenceManagerAsync;
import siena.core.async.SienaFutureProxy;


public class BaseBatchAsync<T> implements BatchAsync<T> {
  
  private Class<T> clazz;
  private PersistenceManagerAsync pm;

  public BaseBatchAsync(PersistenceManagerAsync pm, Class<T> clazz) {
    this.clazz = clazz;
    this.pm = pm;
  }
  
  public BaseBatchAsync(BaseBatchAsync<T> batch) {
    this.clazz = batch.clazz;
    this.pm = batch.pm;
  }
  
  public SienaFuture<Integer> insert(Object... models){
    return pm.insert(models);
  }

  public SienaFuture<Integer> insert(Iterable<T> models){
    return pm.insert(models);
  }

  public SienaFuture<Integer> delete(Object... models){
    return pm.delete(models);
  }

  public SienaFuture<Integer> delete(Iterable<T> models){
    return pm.delete(models);
  }
  
  public SienaFuture<Integer> deleteByKeys(Object... keys){
    return pm.delete(clazz, keys);
  }

  public SienaFuture<Integer> deleteByKeys(Iterable<?> keys){
    return pm.delete(clazz, keys);
  }

  public SienaFuture<Integer> update(T... models) {
    return pm.update(models);
  }

  public SienaFuture<Integer> update(Iterable<T> models) {
    return pm.update(models);
  }
  

  public List<T> get(T... models) {
    return SienaFutureProxy.create(List.class, pm.get(models));
  }

  public List<T> get(Iterable<T> models) {
    return SienaFutureProxy.create(List.class, pm.get(models));
  }

  public SienaFuture<T> getByKey(Object key){
    return pm.getByKey(clazz, key);
  }

  public List<T> getByKeys(Object... keys) {
    return SienaFutureProxy.create(List.class, pm.getByKeys(clazz, keys));
  }

  public List<T> getByKeys(Iterable<?> keys) {
    return SienaFutureProxy.create(List.class, pm.getByKeys(clazz, keys));
  }
  
  public Batch<T> sync() {
    return pm.sync().createBatch(clazz);
  }
}
