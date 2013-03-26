/**
 * 
 */
package siena.core;

import java.lang.reflect.Field;

import siena.BaseQuery;
import siena.ClassInfo;
import siena.PersistenceManager;
import siena.Query;
import siena.Util;

/**
 * @author mandubian <pascal.voitot@mandubian.org>
 *
 */
public class BaseOne<T> implements One4PM<T>{

	transient protected PersistenceManager pm;
	transient protected Class<T> clazz;
	
	protected Relation relation;

	transient protected Query<T> query;
	transient protected T target;
	transient protected boolean isSync = true;
	transient protected boolean isModified = false;
	transient protected T prevTarget;

	public BaseOne(PersistenceManager pm, Class<T> clazz){
		this.pm = pm;
		this.clazz = clazz;
		this.query = pm.createQuery(clazz);
	}

	public BaseOne(PersistenceManager pm, Class<T> clazz, RelationMode mode, Object obj, String fieldName) {
		this.pm = pm;
		this.clazz = clazz;
		switch(mode){
		case AGGREGATION:
			this.relation = new Relation(mode, obj, fieldName);
			this.query = pm.createQuery(clazz).aggregated(obj, fieldName);
			break;
		case RELATION:
			this.query = pm.createQuery(clazz).owned(obj, fieldName);
			break;
		}
		query = pm.createQuery(clazz);		
	}
	
	public T get() {
		sync();
		return target;
	}

	public void set(T obj) {
		this.prevTarget = this.target;

		this.target = obj;
		
		if(relation != null && relation.mode == RelationMode.AGGREGATION){
			// sets relation on target object
			if(this.target != null){
				Util.setField(this.target, ClassInfo.getClassInfo(clazz).aggregator, this.relation);
			}
	
			// resets relation on previous objectll
			if(this.prevTarget != null){
				Util.setField(this.prevTarget, ClassInfo.getClassInfo(clazz).aggregator, null);
			}
		}
		
		isModified = true;
	}

	public One<T> sync() {
		if(!isSync){
			return forceSync();
		}
		return this; 
	}

	public One<T> forceSync() {
		target = query.get();
		isSync = true;
		isModified = false;
		return this;
	}

	public One4PM<T> setSync(boolean isSync) {
		this.isSync = isSync;
		return this;
	}

	public boolean isModified() {
		return isModified;
	}

	public One4PM<T> setModified(boolean isModified) {
		this.isModified = isModified;
		return this;
	}

	public T getPrev() {
		return this.prevTarget;
	}

	public One4PM<T> aggregationMode(Object aggregator, Field field) {
		if(relation == null){
			this.relation = new Relation(RelationMode.AGGREGATION, aggregator, field);
		}else {
			this.relation.mode = RelationMode.AGGREGATION;
			this.relation.target = aggregator;
			this.relation.discriminator = field;
		}
		
		((BaseQuery<T>)(this.query)).aggregated(aggregator, field);
		return this;
	}

	public One4PM<T> relationMode(Object owner, Field field) {
		((BaseQuery<T>)(this.query)).owned(owner, field);
		return this;
	}
}
