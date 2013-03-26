/*
 * Copyright 2008 Alberto Gimeno <gimenete at gmail.com>
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package siena;

import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import siena.ClassInfo.FieldMapKeys;
import siena.core.Aggregator;
import siena.core.Many4PM;
import siena.core.One;
import siena.core.One4PM;
import siena.core.Relation;
import siena.core.RelationMode;
import siena.core.SyncList;
import siena.core.async.ModelAsync;
import siena.core.async.QueryAsync;
import siena.core.batch.Batch;
import siena.core.options.QueryOption;
import siena.core.options.QueryOptionState;

/**
 * This is the base abstract class to extend your domain classes.
 * It's strongly recommended to implement the static method "all". Example:
 *
 * For example:
 * 
 * <code>
 * public static Query&lt;YourClass&gt; all() {
 *		return Model.all(YourClass.class);
 *	}
 * </code>
 * 
 * @author gimenete
 * @author mandubian
 *
 */
public abstract class Model {

	transient private PersistenceManager persistenceManager;
	
	// this is a technical field used by Siena to represent a relation
	// between this model and another one. For ex, it will be used to
	// identify the ancestor in an aggregation relation
	// has transient modifier to prevent serialization FOR THE TIME BEING
	// TODO IS THE TRANSIENT REQUIRED OR CAN IT BE A PB?
	@Aggregator
	private Relation relation;
	
	public Model() {
		init();
	}

	public void get() {
		getPersistenceManager().get(this);
	}

	public void delete() {
		getPersistenceManager().delete(this);
	}

	public void insert() {
		getPersistenceManager().insert(this);
	}

	public void update() {
		getPersistenceManager().update(this);
	}
	
	public void save() {
		getPersistenceManager().save(this);
	}

	public final PersistenceManager getPersistenceManager() {
		if(persistenceManager == null) {
			persistenceManager = PersistenceManagerFactory.getPersistenceManager(getClass());
		}
		return persistenceManager;
	}

	public static <R> Query<R> all(Class<R> clazz) {
		return PersistenceManagerFactory.getPersistenceManager(clazz).createQuery(clazz);
	}
	
	public static <R> Batch<R> batch(Class<R> clazz) {
		return PersistenceManagerFactory.getPersistenceManager(clazz).createBatch(clazz);
	}

	public static <R> R getByKey(Class<R> clazz, Object key) {
		return PersistenceManagerFactory.getPersistenceManager(clazz).getByKey(clazz, key);
	}
	
	public ModelAsync async() {
		return new ModelAsync(this);
	}

	public boolean hasRelation(RelationMode mode) {
		if(mode == RelationMode.AGGREGATION){
			return Util.readField(
					this, ClassInfo.getClassInfo(this.getClass()).aggregator) != null;
		}
		return false;
	}
	
	public Model setRelation(Relation relation){
		Util.setField(this, ClassInfo.getClassInfo(this.getClass()).aggregator, relation);
		
		return this;
	}
	
	public Relation getRelation(){
		return (Relation)Util.readField(this, 
					ClassInfo.getClassInfo(this.getClass()).aggregator);
	}
	
	public Model aggregate(Object aggregator, String fieldName){
		return setRelation(
				new Relation(RelationMode.AGGREGATION, aggregator, Util.getField(aggregator.getClass(), fieldName)));
	}
	
	public boolean equals(Object that) {
		if(this == that) { return true; }
		if(that == null || that.getClass() != this.getClass()) { return false; }

		List<Field> keys = ClassInfo.getClassInfo(getClass()).keys;
		for (Field field : keys) {
			field.setAccessible(true);
			try {
				Object a = field.get(this);
				Object b = field.get(that);
				if(a == null ? b != null : !a.equals(b))
					{ return false; }
			} catch (Exception e) {
				throw new SienaException(e);
			}
		}
		return true;
	}

	public int hashCode() {
		final int prime = 31;
		int result = 1;

		List<Field> keys = ClassInfo.getClassInfo(getClass()).keys;
		for (Field field : keys) {
			field.setAccessible(true);
			try {
				Object value = field.get(this);
				result = prime * result + ((value == null) ? 0 : value.hashCode());
			} catch (Exception e) {
				throw new SienaException(e);
			}
		}
		return result;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void init() {
		// initialize Query<T> types
		Class<?> clazz = getClass();
		
		// Takes into account superclass fields for inheritance!!!!
		ClassInfo info = ClassInfo.getClassInfo(clazz);
		for(Field field:info.queryFieldMap.keySet()){
			try {
				Map<FieldMapKeys, Object> map = info.queryFieldMap.get(field);
				Util.setField(this, field, 
						new ProxyQuery((Class<?>)map.get(FieldMapKeys.CLASS), (String)map.get(FieldMapKeys.FILTER), this));
			} catch (Exception e) {
				throw new SienaException(e);
			}
		}
		
		for(Field field:info.manyFieldMap.keySet()){
			try {
				Map<FieldMapKeys, Object> map = info.manyFieldMap.get(field);
				RelationMode mode = (RelationMode)map.get(FieldMapKeys.MODE);
				switch(mode){
				case AGGREGATION:
					Util.setField(this, field, 
							new ProxyMany((Class<?>)map.get(FieldMapKeys.CLASS), this, 
									(RelationMode)map.get(FieldMapKeys.MODE), field));
					break;
				case RELATION:
					Util.setField(this, field, 
							new ProxyMany((Class<?>)map.get(FieldMapKeys.CLASS), this, 
									(RelationMode)map.get(FieldMapKeys.MODE), (Field)map.get(FieldMapKeys.FIELD)));
					break;
				}				
			} catch (Exception e) {
				throw new SienaException(e);
			}
		}
		
		for(Field field:info.oneFieldMap.keySet()){
			try {
				Map<FieldMapKeys, Object> map = info.oneFieldMap.get(field);
				RelationMode mode = (RelationMode)map.get(FieldMapKeys.MODE);
				switch(mode){
				case AGGREGATION:
					Util.setField(this, field, 
							new ProxyOne((Class<?>)map.get(FieldMapKeys.CLASS), this, 
									(RelationMode)map.get(FieldMapKeys.MODE), field));
					break;
				case RELATION:
					Util.setField(this, field, 
							new ProxyOne((Class<?>)map.get(FieldMapKeys.CLASS), this, 
									(RelationMode)map.get(FieldMapKeys.MODE), (Field)map.get(FieldMapKeys.FIELD)));
					break;
				}				
			} catch (Exception e) {
				throw new SienaException(e);
			}
		}
	}

	class ProxyQuery<T> implements Query<T> {
		private static final long serialVersionUID = -7726081283511624780L;

		private String filter;
		private Class<T> clazz;
		private Model obj;
		private Query<T> query;
		
		public ProxyQuery(Class<T> clazz, String filter, Model obj) {
			this.filter = filter;
			this.clazz = clazz;
			this.obj = obj;
		}

		private Query<T> createQuery() {
			//return getPersistenceManager().createQuery(clazz).filter(filter, obj);
			
			// initializes once the query and reuses it
			// it is not initialized in the constructor because the persistencemanager might not be
			// initialized correctly with the Model
			if(this.query == null){
				this.query = obj.getPersistenceManager().createQuery(clazz);				
			}
			else if(((QueryOptionState)this.query.option(QueryOptionState.ID)).isStateless())
				this.query.resetData();
			return this.query.filter(filter, obj);
		}

		public int count() {
			return createQuery().count();
		}

		@Deprecated
		public int count(int limit) {
			return createQuery().count(limit);
		}

		@Deprecated
		public int count(int limit, Object offset) {
			return createQuery().count(limit, offset);
		}

		public List<T> fetch() {
			return createQuery().fetch();
		}

		public List<T> fetch(int limit) {
			return createQuery().fetch(limit);
		}

		public List<T> fetch(int limit, Object offset) {
			return createQuery().fetch(limit, offset);
		}

		public Query<T> filter(String fieldName, Object value) {
			return createQuery().filter(fieldName, value);
		}

		public Query<T> order(String fieldName) {
			return createQuery().order(fieldName);
		}

		@Deprecated
		public Query<T> search(String match, boolean inBooleanMode, String index) {
			return createQuery().search(match, inBooleanMode, index);
		}
		
		public Query<T> join(String field, String... sortFields) {
			return createQuery().join(field, sortFields);
		}

		public Query<T> aggregated(Object aggregator, String fieldName) {
			return createQuery().aggregated(aggregator, fieldName);
		}
		
		public Query<T> owned(Object owner, String fieldName) {
			return createQuery().owned(owner, fieldName);
		}

		public T get() {
			return createQuery().get();
		}

		public Iterable<T> iter() {
			return createQuery().iter();
		}
		
		public Iterable<T> iter(int limit) {
			return createQuery().iter(limit);
		}
		
		public Iterable<T> iter(int limit, Object offset) {
			return createQuery().iter(limit, offset);
		}
		
		public Iterable<T> iterPerPage(int limit) {
			return createQuery().iterPerPage(limit);
		}	
		
		public ProxyQuery<T> copy() {
			return new ProxyQuery<T>(clazz, filter, obj);
		}

		@Deprecated
		public Object nextOffset() {
			return createQuery().nextOffset();
		}

		public int delete() {
			return createQuery().delete();
		}

		public List<T> fetchKeys() {
			return createQuery().fetchKeys();
		}

		public List<T> fetchKeys(int limit) {
			return createQuery().fetchKeys(limit);
		}

		public List<T> fetchKeys(int limit, Object offset) {
			return createQuery().fetchKeys(limit, offset);
		}

		public List<QueryFilter> getFilters() {
			return createQuery().getFilters();
		}

		public List<QueryOrder> getOrders() {
			return createQuery().getOrders();
		}

		public List<QueryFilterSearch> getSearches() {
			return createQuery().getSearches();
		}

		public List<QueryJoin> getJoins() {
			return createQuery().getJoins();
		}

		public List<QueryAggregated> getAggregatees() {
			return createQuery().getAggregatees();
		}

		public List<QueryOwned> getOwnees() {
			return createQuery().getOwnees();
		}

		@Deprecated
		public void setNextOffset(Object nextOffset) {
			createQuery().setNextOffset(nextOffset);
		}

		public Class<T> getQueriedClass() {
			return clazz;
		}

		public Query<T> paginate(int limit) {
			return createQuery().paginate(limit);
		}

		public Query<T> limit(int limit) {
			return createQuery().limit(limit);
		}

		public Query<T> offset(Object offset) {
			return createQuery().offset(offset);
		}

		public Query<T> customize(QueryOption... options) {
			return createQuery().customize(options);
		}

		public QueryOption option(int option) {
			return createQuery().option(option);
		}

		public Map<Integer, QueryOption> options() {
			return createQuery().options();
		}

		public Query<T> stateful() {
			return createQuery().stateful();
		}

		public Query<T> stateless() {
			return createQuery().stateless();
		}

		public Query<T> release() {
			return createQuery().release();
		}

		public Query<T> resetData() {
			return createQuery().resetData();
		}

		public Query<T> search(String match, String... fields) {
			return createQuery().search(match, fields);
		}

		public Query<T> search(String match, QueryOption opt, String... fields) {
			return createQuery().search(match, opt, fields);
		}

		public int update(Map<String, ?> fieldValues) {
			return createQuery().update(fieldValues);
		}

		public Query<T> nextPage() {
			return createQuery().nextPage();
		}

		public Query<T> previousPage() {
			return createQuery().previousPage();
		}

		public String dump() {
			return createQuery().dump();
		}

		public Query<T> restore(String dump) {
			return createQuery().restore(dump);
		}

		public QueryAsync<T> async() {
			return createQuery().async();
		}

		public T getByKey(Object key) {
			return createQuery().getByKey(key);
		}

		public PersistenceManager getPersistenceManager() {
			return obj.getPersistenceManager();
		}

		public String dump(QueryOption... options) {
			return createQuery().dump(options);
		}

		public void dump(OutputStream os, QueryOption... options) {
			createQuery().dump(os, options);
		}

		public Query<T> restore(String dump, QueryOption... options) {
			return createQuery().restore(dump, options);
		}

		public Query<T> restore(InputStream dump, QueryOption... options) {
			return createQuery().restore(dump, options);
		}

		
	}

	class ProxyMany<T> implements Many4PM<T> {
		private static final long serialVersionUID = -4540064249546783019L;
		
		private Class<T> 		clazz;
		private Model 			obj;
		private Many4PM<T> 		many;
		private RelationMode 	mode;
		private Field			field;	

		public ProxyMany(Class<T> clazz, Model obj, RelationMode mode, Field field) {
			this.clazz = clazz;
			this.obj = obj;
			this.mode = mode;
			this.field = field;
		}

		private Many4PM<T> createMany() {
			if(this.many == null){
				this.many = obj.getPersistenceManager().createMany(clazz);
			}
			//else if(((QueryOptionState)this.listQuery.asQuery().option(QueryOptionState.ID)).isStateless()){
			//	this.listQuery.asQuery().release();				
			//}
			switch(mode){
			case AGGREGATION:
				aggregationMode(obj, field);
				break;
			case RELATION:
				relationMode(obj, field);				
				break;
			}
			
			return this.many;
		}

		public SyncList<T> asList() {
			return createMany().asList();
		}

		public Query<T> asQuery() {
			return createMany().asQuery();
		}

		public List<T> asList2Remove() {
			return ((Many4PM<T>)createMany()).asList2Remove();
		}

		public List<T> asList2Add() {
			return ((Many4PM<T>)createMany()).asList2Add();
		}

		public Many4PM<T> aggregationMode(Object aggregator, Field field) {
			return this.many.aggregationMode(aggregator, field);
		}

		public Many4PM<T> relationMode(Object owner, Field field) {
			return this.many.relationMode(owner, field);
		}

		public Many4PM<T> setSync(boolean isSync) {
			return createMany().setSync(isSync);
		}

	}
	
	
	class ProxyOne<T> implements One4PM<T> {
		
		private Class<T> 		clazz;
		private Model 			ancestor;
		private One4PM<T> 		one;
		private RelationMode 	mode;
		private Field			field;	

		public ProxyOne(Class<T> clazz, Model ancestor, RelationMode mode, Field field) {
			this.clazz = clazz;
			this.ancestor = ancestor;
			this.mode = mode;
			this.field = field;
		}

		private One4PM<T> createOne() {
			if(this.one == null){
				this.one = ancestor.getPersistenceManager().createOne(clazz);
				switch(mode){
				case AGGREGATION:
					aggregationMode(ancestor, field);
					break;
				case RELATION:
					relationMode(ancestor, field);				
					break;
				}
			}
			
			return this.one;
		}

		
		public One4PM<T> aggregationMode(Object aggregator, Field field) {
			return this.one.aggregationMode(aggregator, field);
		}

		public One4PM<T> relationMode(Object owner, Field field) {
			return this.one.relationMode(owner, field);
		}

		public T get() {
			return createOne().get();
		}

		public void set(T obj) {
			createOne().set(obj);
		}

		public One<T> sync() {
			return createOne().sync();
		}

		public One<T> forceSync() {
			return createOne().forceSync();

		}

		public boolean isModified() {
			return createOne().isModified();
		}

		public One4PM<T> setModified(boolean isModified) {
			return createOne().setModified(isModified);

		}

		public T getPrev() {
			return createOne().getPrev();
		}

		public One4PM<T> setSync(boolean isSync) {
			return createOne().setSync(isSync);
		}

	}
}
