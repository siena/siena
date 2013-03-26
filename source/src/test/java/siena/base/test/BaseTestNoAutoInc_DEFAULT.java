package siena.base.test;

import static siena.Json.map;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import siena.Query;
import siena.SienaRestrictedApiException;
import siena.base.test.model.Address;
import siena.base.test.model.AutoInc;
import siena.base.test.model.Contact;
import siena.base.test.model.DataTypes;
import siena.base.test.model.DataTypes.EnumLong;
import siena.base.test.model.Discovery4JoinStringId;
import siena.base.test.model.DiscoveryPrivate;
import siena.base.test.model.DiscoveryStringId;
import siena.base.test.model.MultipleKeys;
import siena.base.test.model.PersonLongAutoID;
import siena.base.test.model.PersonLongManualID;
import siena.base.test.model.PersonStringAutoIncID;
import siena.base.test.model.PersonStringID;
import siena.base.test.model.PersonUUID;
import siena.sdb.SdbPersistenceManager;

public abstract class BaseTestNoAutoInc_DEFAULT extends BaseTestNoAutoInc_BASE {
	

	
	public void testCount() {
		assertEquals(3, pm.createQuery(PersonUUID.class).count());
	}

	public void testFetch() {
		List<PersonUUID> people = queryPersonUUIDOrderBy("n", 0, false).fetch();

		assertNotNull(people);
		assertEquals(3, people.size());

		assertEquals(UUID_TESLA, people.get(0));
		assertEquals(UUID_CURIE, people.get(1));
		assertEquals(UUID_EINSTEIN, people.get(2));
	}
	
	public void testFetchKeys() {
		List<PersonUUID> people = queryPersonUUIDOrderBy("n", 0, false).fetchKeys();

		assertNotNull(people);
		assertEquals(3, people.size());

		assertEquals(UUID_TESLA.id, people.get(0).id);
		assertEquals(UUID_CURIE.id, people.get(1).id);
		assertEquals(UUID_EINSTEIN.id, people.get(2).id);
		
		assertTrue(people.get(0).isOnlyIdFilled());
		assertTrue(people.get(1).isOnlyIdFilled());
		assertTrue(people.get(2).isOnlyIdFilled());

	}

	public void testFetchOrder() {
		List<PersonUUID> people = queryPersonUUIDOrderBy("firstName", "", false).fetch();

		assertNotNull(people);
		assertEquals(3, people.size());

		assertEquals(UUID_EINSTEIN, people.get(0));
		assertEquals(UUID_CURIE, people.get(1));
		assertEquals(UUID_TESLA, people.get(2));
	}
	
	public void testFetchOrderKeys() {
		List<PersonUUID> people = queryPersonUUIDOrderBy("firstName", "", false).fetchKeys();

		assertNotNull(people);
		assertEquals(3, people.size());

		assertEquals(UUID_EINSTEIN.id, people.get(0).id);
		assertEquals(UUID_CURIE.id, people.get(1).id);
		assertEquals(UUID_TESLA.id, people.get(2).id);
	}

	public void testFetchOrderDesc() {
		List<PersonUUID> people = queryPersonUUIDOrderBy("lastName", "", true).fetch();

		assertNotNull(people);
		assertEquals(3, people.size());

		assertEquals(UUID_TESLA, people.get(0));
		assertEquals(UUID_EINSTEIN, people.get(1));
		assertEquals(UUID_CURIE, people.get(2));
	}

	public void testFetchOrderDescKeys() {
		List<PersonUUID> people = queryPersonUUIDOrderBy("lastName", "", true).fetchKeys();

		assertNotNull(people);
		assertEquals(3, people.size());

		assertEquals(UUID_TESLA.id, people.get(0).id);
		assertEquals(UUID_EINSTEIN.id, people.get(1).id);
		assertEquals(UUID_CURIE.id, people.get(2).id);
	}
	
	public void testFetchOrderOnLongAutoId() {
		List<PersonLongAutoID> people = queryPersonLongAutoIDOrderBy("id", "", false).fetchKeys();
		assertEquals(0, people.size());
	}

	public void testFetchOrderOnLongManualId() {
		List<PersonLongManualID> people = queryPersonLongManualIDOrderBy("id", "", false).fetchKeys();

		assertNotNull(people);
		assertEquals(3, people.size());

		assertEquals(LongManualID_TESLA.id, people.get(0).id);
		assertEquals(LongManualID_CURIE.id, people.get(1).id);
		assertEquals(LongManualID_EINSTEIN.id, people.get(2).id);
	}
	
	public void testFetchOrderOnStringId() {
		List<PersonStringID> people = queryPersonStringIDOrderBy("id", "", false).fetchKeys();

		assertNotNull(people);
		assertEquals(3, people.size());

		assertEquals(StringID_CURIE.id, people.get(0).id);
		assertEquals(StringID_EINSTEIN.id, people.get(1).id);
		assertEquals(StringID_TESLA.id, people.get(2).id);
	}
		
	public void testFetchOrderOnUUID() {
		List<PersonUUID> l = getOrderedPersonUUIDs();
		List<PersonUUID> people = queryPersonUUIDOrderBy("id", "", false).fetchKeys();

		assertNotNull(people);
		assertEquals(3, people.size());

		assertEquals(l.get(0).id, people.get(0).id);
		assertEquals(l.get(1).id, people.get(1).id);
		assertEquals(l.get(2).id, people.get(2).id);
	}
	
	public void testFetchOrderOnLongAutoIdDesc() {
		List<PersonLongAutoID> people = queryPersonLongAutoIDOrderBy("id", "", true).fetchKeys();

		assertEquals(0, people.size());
	}
		
	public void testFetchOrderOnLongManualIdDesc() {
		List<PersonLongManualID> people = queryPersonLongManualIDOrderBy("id", "", true).fetchKeys();

		assertNotNull(people);
		assertEquals(3, people.size());

		assertEquals(LongManualID_EINSTEIN.id, people.get(0).id);
		assertEquals(LongManualID_CURIE.id, people.get(1).id);
		assertEquals(LongManualID_TESLA.id, people.get(2).id);
	}
	
	public void testFetchOrderOnStringIdDesc() {
		List<PersonStringID> people = queryPersonStringIDOrderBy("id", "", true).fetchKeys();

		assertNotNull(people);
		assertEquals(3, people.size());

		assertEquals(StringID_TESLA.id, people.get(0).id);
		assertEquals(StringID_EINSTEIN.id, people.get(1).id);
		assertEquals(StringID_CURIE.id, people.get(2).id);
	}
	
	public void testFetchOrderOnUUIDDesc() {
		List<PersonUUID> l = getOrderedPersonUUIDs();
		List<PersonUUID> people = queryPersonUUIDOrderBy("id", "", true).fetchKeys();

		assertNotNull(people);
		assertEquals(3, people.size());

		assertEquals(l.get(2).id, people.get(0).id);
		assertEquals(l.get(1).id, people.get(1).id);
		assertEquals(l.get(0).id, people.get(2).id);
	}
	
	
	public void testFilterOperatorEqualString() {
		PersonUUID person = pm.createQuery(PersonUUID.class).filter("firstName", "Albert").get();
		assertNotNull(person);
		assertEquals(UUID_EINSTEIN, person);
	}
	
	public void testFilterOperatorEqualInt() {
		PersonUUID person = pm.createQuery(PersonUUID.class).filter("n", 3).get();
		assertNotNull(person);
		assertEquals(UUID_EINSTEIN, person);
	}
	
	public void testFilterOperatorEqualUUID() {
		List<PersonUUID> l = getOrderedPersonUUIDs();
	
		PersonUUID person = pm.createQuery(PersonUUID.class).filter("id", l.get(0).id).get();
		assertNotNull(person);
		assertEquals(l.get(0), person);
	}
	
	public void testFilterOperatorEqualLongAutoID() {
		try {
			PersonLongAutoID person = pm.createQuery(PersonLongAutoID.class).filter("id", LongAutoID_EINSTEIN.id).get();
		}catch(SienaRestrictedApiException ex){
			return;
		}
		fail();
	}
		
	public void testFilterOperatorEqualLongManualID() {
		PersonLongManualID person = pm.createQuery(PersonLongManualID.class).filter("id", 3L).get();
		assertNotNull(person);
		assertEquals(LongManualID_EINSTEIN, person);
	}
	
	public void testFilterOperatorEqualStringID() {
		PersonStringID person = pm.createQuery(PersonStringID.class).filter("id", "EINSTEIN").get();
		assertNotNull(person);
		assertEquals(StringID_EINSTEIN, person);
	}
	
	public void testFilterOperatorNotEqualString() {
		List<PersonStringID> people = pm.createQuery(PersonStringID.class).filter("firstName!=", "Albert").order("firstName").fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(StringID_CURIE, people.get(0));
		assertEquals(StringID_TESLA, people.get(1));
	}
	
	public void testFilterOperatorNotEqualInt() {
		List<PersonUUID> people = pm.createQuery(PersonUUID.class).filter("n!=", 3).order("n").fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(UUID_TESLA, people.get(0));
		assertEquals(UUID_CURIE, people.get(1));
	}

	public void testFilterOperatorNotEqualUUID() {
		List<PersonUUID> l = getOrderedPersonUUIDs();
		List<PersonUUID> people = pm.createQuery(PersonUUID.class).filter("id!=", l.get(0).id).order("id").fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(l.get(1), people.get(0));
		assertEquals(l.get(2), people.get(1));
	}
	
	public void testFilterOperatorNotEqualLongAutoID() {
		List<PersonLongAutoID> people = pm.createQuery(PersonLongAutoID.class).filter("id!=", LongAutoID_EINSTEIN.id).order("id").fetch();
		assertEquals(0, people.size());
	}

	public void testFilterOperatorNotEqualLongManualID() {
		List<PersonLongManualID> people = pm.createQuery(PersonLongManualID.class).filter("id!=", 3L).order("id").fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(LongManualID_TESLA, people.get(0));
		assertEquals(LongManualID_CURIE, people.get(1));
	}
	
	public void testFilterOperatorNotEqualStringID() {
		List<PersonStringID> people = pm.createQuery(PersonStringID.class).filter("id!=", StringID_EINSTEIN.id).order("id").fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(StringID_CURIE, people.get(0));
		assertEquals(StringID_TESLA, people.get(1));
	}

	public void testFilterOperatorIn() {
		@SuppressWarnings("serial")
		List<PersonUUID> people = 
			pm.createQuery(PersonUUID.class)
				.filter("n IN", new ArrayList<Integer>(){{ 
					add(2);
					add(3);
				}})
				.order("n")
				.fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(UUID_CURIE, people.get(0));
		assertEquals(UUID_EINSTEIN, people.get(1));
	}
	
	public void testFilterOperatorInOrder() {
		@SuppressWarnings("serial")
		List<PersonUUID> people = 
			pm.createQuery(PersonUUID.class)
				.filter("n IN", new ArrayList<Integer>(){{ 
					add(3);
					add(2);
				}})
				.order("n")
				.fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(UUID_CURIE, people.get(0));
		assertEquals(UUID_EINSTEIN, people.get(1));
	}
	
	public void testFilterOperatorInForUUID() {
		List<PersonUUID> l = getOrderedPersonUUIDs();
		
		List<PersonUUID> people = 
			pm.createQuery(PersonUUID.class)
				.filter("id IN", Arrays.asList( l.get(0).id, l.get(1).id))
				.order("id")
				.fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(l.get(0), people.get(0));
		assertEquals(l.get(1), people.get(1));
	}
	
	public void testFilterOperatorInForLongAutoID() {
		try {
			@SuppressWarnings("serial")
			List<PersonLongAutoID> people = 
				pm.createQuery(PersonLongAutoID.class)
					.filter("id IN", new ArrayList<Long>(){{ 
						add(LongAutoID_TESLA.id);
						add(LongAutoID_CURIE.id);
					}})
					.fetch();
		}catch(SienaRestrictedApiException ex){
			return;
		}

		fail();
	}

	public void testFilterOperatorInForLongManualID() {
		@SuppressWarnings("serial")
		List<PersonLongManualID> people = 
			pm.createQuery(PersonLongManualID.class)
				.filter("id IN", new ArrayList<Long>(){{ 
					add(LongManualID_TESLA.id);
					add(LongManualID_CURIE.id);
				}})
				.fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(LongManualID_TESLA, people.get(0));
		assertEquals(LongManualID_CURIE, people.get(1));
	}
	
	public void testFilterOperatorInForStringID() {
		@SuppressWarnings("serial")
		List<PersonStringID> people = 
			pm.createQuery(PersonStringID.class)
				.filter("id IN", new ArrayList<String>(){{ 
					add(StringID_TESLA.id);
					add(StringID_CURIE.id);
				}})
				.order("id")
				.fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(StringID_CURIE, people.get(0));
		assertEquals(StringID_TESLA, people.get(1));
	}
	
	public void testFilterOperatorLessThan() {
		List<PersonUUID> people = pm.createQuery(PersonUUID.class).filter("n<", 3).order("n").fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(UUID_TESLA, people.get(0));
		assertEquals(UUID_CURIE, people.get(1));
	}
	
	public void testFilterOperatorLessThanForUUID() {
		List<PersonUUID> l = getOrderedPersonUUIDs();

		List<PersonUUID> people = pm.createQuery(PersonUUID.class).filter("id<", l.get(2).id).order("id").fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		
		assertEquals(l.get(0), people.get(0));
		assertEquals(l.get(1), people.get(1));
	}
	
	public void testFilterOperatorLessThanForLongAutoID() {
		try {
			List<PersonLongAutoID> people = pm.createQuery(PersonLongAutoID.class).filter("id<", LongAutoID_EINSTEIN.id).order("id").fetch();
		}catch(SienaRestrictedApiException ex){
			return;
		}

		fail();
	}
	
	public void testFilterOperatorLessThanForLongManualID() {
		List<PersonLongManualID> people = pm.createQuery(PersonLongManualID.class).filter("id<", 3L).order("id").fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(LongManualID_TESLA, people.get(0));
		assertEquals(LongManualID_CURIE, people.get(1));
	}
	
	public void testFilterOperatorLessThanForStringID() {
		List<PersonStringID> people = pm.createQuery(PersonStringID.class).filter("id<", StringID_TESLA.id).order("id").fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(StringID_CURIE, people.get(0));
		assertEquals(StringID_EINSTEIN, people.get(1));
	}
	
	public void testFilterOperatorLessThanOrEqual() {
		List<PersonUUID> people = pm.createQuery(PersonUUID.class).filter("n<=", 3).order("n").fetch();

		assertNotNull(people);
		assertEquals(3, people.size());

		assertEquals(UUID_TESLA, people.get(0));
		assertEquals(UUID_CURIE, people.get(1));
		assertEquals(UUID_EINSTEIN, people.get(2));		
	}
	
	public void testFilterOperatorLessThanOrEqualForUUID() {
		List<PersonUUID> l = getOrderedPersonUUIDs();

		List<PersonUUID> people = pm.createQuery(PersonUUID.class).filter("id<=", l.get(2).id).order("id").fetch();

		assertNotNull(people);
		assertEquals(3, people.size());

		
		assertEquals(l.get(0), people.get(0));
		assertEquals(l.get(1), people.get(1));
		assertEquals(l.get(2), people.get(2));
	}
	
	public void testFilterOperatorLessThanOrEqualForLongAutoID() {
		try {
			List<PersonLongAutoID> people = pm.createQuery(PersonLongAutoID.class).filter("id<=", LongAutoID_EINSTEIN.id).order("id").fetch();
		}catch(SienaRestrictedApiException ex){
			return;
		}

		fail();
	}
	
	public void testFilterOperatorLessThanOrEqualForLongManualID() {
		List<PersonLongManualID> people = pm.createQuery(PersonLongManualID.class).filter("id<=", LongManualID_EINSTEIN.id).order("id").fetch();

		assertNotNull(people);
		assertEquals(3, people.size());

		assertEquals(LongManualID_TESLA, people.get(0));
		assertEquals(LongManualID_CURIE, people.get(1));
		assertEquals(LongManualID_EINSTEIN, people.get(2));
	}
	
	public void testFilterOperatorLessThanOrEqualForStringID() {
		List<PersonStringID> people = pm.createQuery(PersonStringID.class).filter("id<=", StringID_TESLA.id).order("id").fetch();

		assertNotNull(people);
		assertEquals(3, people.size());

		assertEquals(StringID_CURIE, people.get(0));
		assertEquals(StringID_EINSTEIN, people.get(1));
		assertEquals(StringID_TESLA, people.get(2));
	}
	
	
	public void testFilterOperatorMoreThan() {
		List<PersonUUID> people = pm.createQuery(PersonUUID.class).filter("n>", 1).order("n").fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(UUID_CURIE, people.get(0));
		assertEquals(UUID_EINSTEIN, people.get(1));
	}
	
	public void testFilterOperatorMoreThanForUUID() {
		List<PersonUUID> l = getOrderedPersonUUIDs();

		List<PersonUUID> people = pm.createQuery(PersonUUID.class).filter("id>", l.get(0).id).order("id").fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		
		assertEquals(l.get(1), people.get(0));
		assertEquals(l.get(2), people.get(1));
	}
	
	public void testFilterOperatorMoreThanForLongAutoID() {
		try {
			List<PersonLongAutoID> people = pm.createQuery(PersonLongAutoID.class).filter("id>", LongAutoID_TESLA.id).order("id").fetch();
		}catch(SienaRestrictedApiException ex){
			return;
		}

		fail();
	}
	
	public void testFilterOperatorMoreThanForLongManualID() {
		List<PersonLongManualID> people = pm.createQuery(PersonLongManualID.class).filter("id>", LongManualID_TESLA.id).order("id").fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(LongManualID_CURIE, people.get(0));
		assertEquals(LongManualID_EINSTEIN, people.get(1));
	}
	
	public void testFilterOperatorMoreThanForStringID() {
		List<PersonStringID> people = pm.createQuery(PersonStringID.class).filter("id>", StringID_CURIE.id).order("id").fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(StringID_EINSTEIN, people.get(0));
		assertEquals(StringID_TESLA, people.get(1));
	}

	
	public void testFilterOperatorMoreThanOrEqual() {
		List<PersonUUID> people = pm.createQuery(PersonUUID.class).filter("n>=", 1).order("n").fetch();

		assertNotNull(people);
		assertEquals(3, people.size());

		assertEquals(UUID_TESLA, people.get(0));
		assertEquals(UUID_CURIE, people.get(1));
		assertEquals(UUID_EINSTEIN, people.get(2));
	}

	public void testFilterOperatorMoreThanOrEqualForUUID() {
		List<PersonUUID> l = getOrderedPersonUUIDs();

		List<PersonUUID> people = pm.createQuery(PersonUUID.class).filter("id>=", l.get(0).id).order("id").fetch();

		assertNotNull(people);
		assertEquals(3, people.size());

		
		assertEquals(l.get(0), people.get(0));
		assertEquals(l.get(1), people.get(1));
		assertEquals(l.get(2), people.get(2));
	}
	
	public void testFilterOperatorMoreThanOrEqualForLongAutoID() {
		try {
			List<PersonLongAutoID> people = pm.createQuery(PersonLongAutoID.class).filter("id>=", LongAutoID_CURIE.id).order("id").fetch();
		}catch(SienaRestrictedApiException ex){
			return;
		}

		fail();
	}
	
	public void testFilterOperatorMoreThanOrEqualForLongManualID() {
		List<PersonLongManualID> people = pm.createQuery(PersonLongManualID.class).filter("id>=", LongManualID_CURIE.id).order("id").fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(LongManualID_CURIE, people.get(0));
		assertEquals(LongManualID_EINSTEIN, people.get(1));
	}
	
	public void testFilterOperatorMoreThanOrEqualForStringID() {
		List<PersonStringID> people = pm.createQuery(PersonStringID.class).filter("id>=", StringID_EINSTEIN.id).order("id").fetch();

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(StringID_EINSTEIN, people.get(0));
		assertEquals(StringID_TESLA, people.get(1));
	}
	
	public void testCountFilter() {
		assertEquals(2, pm.createQuery(PersonUUID.class).filter("n<", 3).count());
	}

	public void testCountFilterUUID() {
		List<PersonUUID> l = getOrderedPersonUUIDs();
		assertEquals(2, pm.createQuery(PersonUUID.class).filter("id<", l.get(2).id).count());
	}
	
	public void testCountFilterLongAutoID() {
		try {
			assertEquals(2, pm.createQuery(PersonLongAutoID.class).filter("id<", LongAutoID_EINSTEIN.id).count());
		}catch(SienaRestrictedApiException ex){
			return;
		}
		fail();
	}

	public void testCountFilterLongManualID() {
		assertEquals(2, pm.createQuery(PersonLongManualID.class).filter("id<", LongManualID_EINSTEIN.id).count());
	}
	
	public void testCountFilterStringID() {
		assertEquals(2, pm.createQuery(PersonStringID.class).filter("id<", StringID_TESLA.id).count());
	}
	
	public void testFetchLimit() {
		List<PersonUUID> people = queryPersonUUIDOrderBy("n", 0, false).fetch(1);

		assertNotNull(people);
		assertEquals(1, people.size());

		assertEquals(UUID_TESLA, people.get(0));
	}

	public void testFetchLimitUUID() {
		List<PersonUUID> l = getOrderedPersonUUIDs();
		List<PersonUUID> people = queryPersonUUIDOrderBy("id", l.get(0), false).fetch(1);

		assertNotNull(people);
		assertEquals(1, people.size());

		assertEquals(l.get(0), people.get(0));
	}
	
	public void testFetchLimitLongAutoID() {
		List<PersonLongAutoID> people = queryPersonLongAutoIDOrderBy("id", 0, false).fetch(1);

		assertNotNull(people);
		assertEquals(1, people.size());

		assertEquals(LongAutoID_TESLA, people.get(0));
	}
	
	public void testFetchLimitLongManualID() {
		List<PersonLongManualID> people = queryPersonLongManualIDOrderBy("id", 0, false).fetch(1);

		assertNotNull(people);
		assertEquals(1, people.size());

		assertEquals(LongManualID_TESLA, people.get(0));
	}
	
	public void testFetchLimitStringID() {
		List<PersonStringID> people = queryPersonStringIDOrderBy("id", StringID_CURIE, false).fetch(1);

		assertNotNull(people);
		assertEquals(1, people.size());

		assertEquals(StringID_CURIE, people.get(0));
	}
/*	
	@Deprecated
	public void testCountLimit() {
		assertEquals(1, pm.createQuery(PersonUUID.class).filter("n<", 3).count(1));
	}
*/
	public void testFetchLimitReal() {
		DiscoveryStringId[] discs = new DiscoveryStringId[10];
		for(int i=0; i<10; i++){
			discs[i] = new DiscoveryStringId("Disc_"+i, StringID_CURIE);
			pm.insert(discs[i]);
		}

		List<DiscoveryStringId> res = pm.createQuery(DiscoveryStringId.class).order("name").fetch(3);
		assertNotNull(res);
		assertEquals(3, res.size());
		
		assertEquals(discs[0], res.get(0));
		assertEquals(discs[1], res.get(1));
		assertEquals(discs[2], res.get(2));
	}
	

	public void testFetchLimitOffsetReal() {
		DiscoveryStringId[] discs = new DiscoveryStringId[10];
		for(int i=0; i<10; i++){
			discs[i] = new DiscoveryStringId("Disc_"+i, StringID_CURIE);
			pm.insert(discs[i]);
		}

		List<DiscoveryStringId> res = pm.createQuery(DiscoveryStringId.class).order("name").fetch(3, 5);
		assertNotNull(res);
		assertEquals(3, res.size());
		
		assertEquals(discs[5], res.get(0));
		assertEquals(discs[6], res.get(1));
		assertEquals(discs[7], res.get(2));
	}
	
	public void testFetchLimitOffset() {
		Query<PersonUUID> query = queryPersonUUIDOrderBy("n", 0, false);
		query.fetch(1);
		List<PersonUUID> people = query.fetch(2, 1);

		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(UUID_CURIE, people.get(0));
		assertEquals(UUID_EINSTEIN, people.get(1));
	}

/*	@Deprecated
	public void testCountLimitOffset() {
		Query<PersonUUID> query = queryPersonUUIDOrderBy("n", 0, false);
		query.fetch(1);
		assertEquals(2, query.count(2, 1));
	}
*/
	public void testInsertUUID() {
		PersonUUID maxwell = new PersonUUID();
		maxwell.firstName = "James Clerk";
		maxwell.lastName = "Maxwell";
		maxwell.city = "Edinburgh";
		maxwell.n = 4;

		pm.insert(maxwell);
		assertNotNull(maxwell.id);

		List<PersonUUID> people = queryPersonUUIDOrderBy("n", 0, false).fetch();
		assertEquals(4, people.size());

		assertEquals(UUID_TESLA, people.get(0));
		assertEquals(UUID_CURIE, people.get(1));
		assertEquals(UUID_EINSTEIN, people.get(2));
		assertEquals(maxwell, people.get(3));
	}

	public void testInsertLongAutoID() {
		PersonLongAutoID maxwell = new PersonLongAutoID();
		maxwell.firstName = "James Clerk";
		maxwell.lastName = "Maxwell";
		maxwell.city = "Edinburgh";
		maxwell.n = 4;

		pm.insert(maxwell);
		assertNotNull(maxwell.id);

		List<PersonLongAutoID> people = queryPersonLongAutoIDOrderBy("n", 0, false).fetch();
		assertEquals(4, people.size());

		assertEquals(LongAutoID_TESLA, people.get(0));
		assertEquals(LongAutoID_CURIE, people.get(1));
		assertEquals(LongAutoID_EINSTEIN, people.get(2));
		assertEquals(maxwell, people.get(3));
	}

	public void testInsertLongManualID() {
		PersonLongManualID maxwell = new PersonLongManualID();
		maxwell.id = 4L;
		maxwell.firstName = "James Clerk";
		maxwell.lastName = "Maxwell";
		maxwell.city = "Edinburgh";
		maxwell.n = 4;

		pm.insert(maxwell);
		assertEquals((Long)4L, maxwell.id);

		List<PersonLongManualID> people = queryPersonLongManualIDOrderBy("n", 0, false).fetch();
		assertEquals(4, people.size());

		assertEquals(LongManualID_TESLA, people.get(0));
		assertEquals(LongManualID_CURIE, people.get(1));
		assertEquals(LongManualID_EINSTEIN, people.get(2));
		assertEquals(maxwell, people.get(3));
	}
	
	public void testInsertStringID() {
		PersonStringID maxwell = new PersonStringID();
		maxwell.id = "MAXWELL";
		maxwell.firstName = "James Clerk";
		maxwell.lastName = "Maxwell";
		maxwell.city = "Edinburgh";
		maxwell.n = 4;

		pm.insert(maxwell);
		assertEquals(maxwell.id, "MAXWELL");

		List<PersonStringID> people = queryPersonStringIDOrderBy("n", 0, false).fetch();
		assertEquals(4, people.size());

		assertEquals(StringID_TESLA, people.get(0));
		assertEquals(StringID_CURIE, people.get(1));
		assertEquals(StringID_EINSTEIN, people.get(2));
		assertEquals(maxwell, people.get(3));
	}
	
	
	public void testGetUUID() {
		PersonUUID curie = getPersonUUID(UUID_CURIE.id);
		assertEquals(UUID_CURIE, curie);
	}

	public void testGetLongAutoID() {
		PersonLongAutoID curie = getPersonLongAutoID(LongAutoID_CURIE.id);
		assertEquals(LongAutoID_CURIE, curie);
	}

	public void testGetLongManualID() {
		PersonLongManualID curie = getPersonLongManualID(LongManualID_CURIE.id);
		assertEquals(LongManualID_CURIE, curie);
	}

	public void testGetStringID() {
		PersonStringID curie = getPersonStringID(StringID_CURIE.id);
		assertEquals(StringID_CURIE, curie);
	}
	
	public void testUpdateUUID() {
		PersonUUID curie = getPersonUUID(UUID_CURIE.id);
		curie.lastName = "Sklodowska–Curie";
		pm.update(curie);
		PersonUUID curie2 = getPersonUUID(UUID_CURIE.id);
		assertEquals(curie2, curie);
	}

	public void testUpdateLongAutoID() {
		PersonLongAutoID curie = getPersonLongAutoID(LongAutoID_CURIE.id);
		curie.lastName = "Sklodowska–Curie";
		pm.update(curie);
		PersonLongAutoID curie2 = getPersonLongAutoID(LongAutoID_CURIE.id);
		assertEquals(curie2, curie);
	}
	
	public void testDeleteUUID() {
		PersonUUID curie = getPersonUUID(UUID_CURIE.id);
		pm.delete(curie);
		
		List<PersonUUID> people = queryPersonUUIDOrderBy("n", 0, false).fetch();
		assertNotNull(people);
		assertEquals(2, people.size());

		assertEquals(UUID_TESLA, people.get(0));
		assertEquals(UUID_EINSTEIN, people.get(1));
	}

	
	public void testIterFullUUID() {
		Iterable<PersonUUID> people = pm.createQuery(PersonUUID.class).order("n").iter();

		assertNotNull(people);

		@SuppressWarnings("serial")
		ArrayList<PersonUUID> l = new ArrayList<PersonUUID>() {{ 
			add(UUID_TESLA); 
			add(UUID_CURIE);
			add(UUID_EINSTEIN);
		}};
		
		int i = 0;
		for (PersonUUID person : people) {
			PersonUUID p = l.get(i);
			assertEquals(p, person);
			i++;
		}
	}
	
	public void testIterFullLongAutoID() {
		Iterable<PersonLongAutoID> people = pm.createQuery(PersonLongAutoID.class).order("n").iter();

		assertNotNull(people);

		PersonLongAutoID[] array = new PersonLongAutoID[] { LongAutoID_TESLA, LongAutoID_CURIE, LongAutoID_EINSTEIN };

		int i = 0;
		for (PersonLongAutoID person : people) {
			assertEquals(array[i], person);
			i++;
		}
	}

	public void testIterFullLongManualID() {
		Iterable<PersonLongManualID> people = pm.createQuery(PersonLongManualID.class).order("n").iter();

		assertNotNull(people);

		PersonLongManualID[] array = new PersonLongManualID[] { LongManualID_TESLA, LongManualID_CURIE, LongManualID_EINSTEIN };

		int i = 0;
		for (PersonLongManualID person : people) {
			assertEquals(array[i], person);
			i++;
		}
	}
	
	public void testIterFullLongStringID() {
		Iterable<PersonStringID> people = pm.createQuery(PersonStringID.class).order("n").iter();

		assertNotNull(people);

		PersonStringID[] array = new PersonStringID[] { StringID_TESLA, StringID_CURIE, StringID_EINSTEIN  };

		int i = 0;
		for (PersonStringID person : people) {
			assertEquals(array[i], person);
			i++;
		}
	}
	
	public void testIterLimitUUID() {
		Iterable<PersonUUID> people = pm.createQuery(PersonUUID.class).order("n").iter(2);

		assertNotNull(people);

		@SuppressWarnings("serial")
		ArrayList<PersonUUID> l = new ArrayList<PersonUUID>() {{ 
			add(UUID_TESLA); 
			add(UUID_CURIE);
		}};
		
		int i = 0;
		for (PersonUUID person : people) {
			assertEquals( l.get(i), person);
			i++;
		}
	}
	
	public void testIterLimitLongAutoID() {
		Iterable<PersonLongAutoID> people = pm.createQuery(PersonLongAutoID.class).order("n").iter(2);

		assertNotNull(people);

		PersonLongAutoID[] array = new PersonLongAutoID[] { LongAutoID_TESLA, LongAutoID_CURIE };

		int i = 0;
		for (PersonLongAutoID person : people) {
			assertEquals(array[i], person);
			i++;
		}
	}

	public void testIterLimitLongManualID() {
		Iterable<PersonLongManualID> people = pm.createQuery(PersonLongManualID.class).order("n").iter(2);

		assertNotNull(people);

		PersonLongManualID[] array = new PersonLongManualID[] { LongManualID_TESLA, LongManualID_CURIE };

		int i = 0;
		for (PersonLongManualID person : people) {
			assertEquals(array[i], person);
			i++;
		}
	}
	
	public void testIterLimitLongStringID() {
		Iterable<PersonStringID> people = pm.createQuery(PersonStringID.class).order("n").iter(2);

		assertNotNull(people);

		PersonStringID[] array = new PersonStringID[] { StringID_TESLA, StringID_CURIE };

		int i = 0;
		for (PersonStringID person : people) {
			assertEquals(array[i], person);
			i++;
		}
	}
	
	public void testIterLimitOffsetUUID() {
		Iterable<PersonUUID> people = pm.createQuery(PersonUUID.class).order("n").iter(2,1);

		assertNotNull(people);

		@SuppressWarnings("serial")
		ArrayList<PersonUUID> l = new ArrayList<PersonUUID>() {{ 
			add(UUID_CURIE);
			add(UUID_EINSTEIN);
		}};
		
		int i = 0;
		for (PersonUUID person : people) {
			assertEquals( l.get(i), person);
			i++;
		}
	}
	
	public void testIterLimitOffsetLongAutoID() {
		Iterable<PersonLongAutoID> people = pm.createQuery(PersonLongAutoID.class).order("n").iter(2, 1);

		assertNotNull(people);

		PersonLongAutoID[] array = new PersonLongAutoID[] { LongAutoID_CURIE, LongAutoID_EINSTEIN };

		int i = 0;
		for (PersonLongAutoID person : people) {
			assertEquals(array[i], person);
			i++;
		}
	}

	public void testIterLimitOffsetLongManualID() {
		Iterable<PersonLongManualID> people = pm.createQuery(PersonLongManualID.class).order("n").iter(2,1);

		assertNotNull(people);

		PersonLongManualID[] array = new PersonLongManualID[] { LongManualID_CURIE, LongManualID_EINSTEIN };

		int i = 0;
		for (PersonLongManualID person : people) {
			assertEquals(array[i], person);
			i++;
		}
	}
	
	public void testIterLimitOffsetLongStringID() {
		Iterable<PersonStringID> people = pm.createQuery(PersonStringID.class).order("n").iter(2,1);

		assertNotNull(people);

		PersonStringID[] array = new PersonStringID[] { StringID_CURIE, StringID_EINSTEIN };

		int i = 0;
		for (PersonStringID person : people) {
			assertEquals(array[i], person);
			i++;
		}
	}
	
	public void testIterFilter() {
		Iterable<PersonUUID> people = pm.createQuery(PersonUUID.class).filter("n>", 1).order("n").iter();

		assertNotNull(people);

		PersonUUID[] array = new PersonUUID[] { UUID_CURIE, UUID_EINSTEIN };

		int i = 0;
		for (PersonUUID PersonIntKey : people) {
			assertEquals(array[i], PersonIntKey);
			i++;
		}
	}
	
	public void testIterFilterLimit() {
		Iterable<PersonUUID> people = pm.createQuery(PersonUUID.class).filter("n>", 1).order("n").iter(1);

		assertNotNull(people);

		PersonUUID[] array = new PersonUUID[] { UUID_CURIE };

		int i = 0;
		for (PersonUUID PersonIntKey : people) {
			assertEquals(array[i], PersonIntKey);
			i++;
		}
	}
	
	public void testIterFilterLimitOffset() {
		Iterable<PersonUUID> people = pm.createQuery(PersonUUID.class).filter("n>", 1).order("n").iter(2, 1);

		assertNotNull(people);

		PersonUUID[] array = new PersonUUID[] { UUID_EINSTEIN };

		int i = 0;
		for (PersonUUID PersonIntKey : people) {
			assertEquals(array[i], PersonIntKey);
			i++;
		}
	}
	
	public void testOrderLongAutoId() {
		List<PersonLongAutoID> people = queryPersonLongAutoIDOrderBy("id", "", false).fetch();
		
		assertNotNull(people);
		assertEquals(3, people.size());
		
		PersonLongAutoID[] array = new PersonLongAutoID[] { LongAutoID_TESLA, LongAutoID_CURIE, LongAutoID_EINSTEIN };

		int i = 0;
		for (PersonLongAutoID person : people) {
			assertEquals(array[i], person);
			i++;
		}
	}
	
	public void testOrderLongManualId() {
		List<PersonLongManualID> people = queryPersonLongManualIDOrderBy("id", "", false).fetch();
		
		assertNotNull(people);
		assertEquals(3, people.size());
		
		PersonLongManualID[] array = new PersonLongManualID[] { LongManualID_TESLA, LongManualID_CURIE, LongManualID_EINSTEIN };

		int i = 0;
		for (PersonLongManualID person : people) {
			assertEquals(array[i], person);
			i++;
		}
	}
	
	public void testOrderStringId() {
		List<PersonStringID> people = queryPersonStringIDOrderBy("id", "", false).fetch();
		
		assertNotNull(people);
		assertEquals(3, people.size());
		
		PersonStringID[] array = new PersonStringID[] { StringID_CURIE, StringID_EINSTEIN, StringID_TESLA };

		int i = 0;
		for (PersonStringID person : people) {
			assertEquals(array[i], person);
			i++;
		}
	}
	
	public void testGetObjectNotFound() {
		try {
			getPersonUUID("");
			fail();
		} catch(Exception e) {
			System.out.println("Everything is OK");
		}
		
		assertNull(pm.createQuery(PersonUUID.class).filter("firstName", "John").get());
	}
	
	public void testDeleteObjectNotFound() {
		try {
			PersonUUID p = new PersonUUID();
			pm.delete(p);
			fail();
		} catch(Exception e) {
			System.out.println("Everything is OK");
		}
	}
	
	public void testAutoincrement() {
		if(!supportsAutoincrement()) return;

		AutoInc first = new AutoInc();
		first.name = "first";
		pm.insert(first);
		assertTrue(first.id > 0);

		AutoInc second = new AutoInc();
		second.name = "second";
		pm.insert(second);
		assertTrue(second.id > 0);
		
		assertTrue(second.id > first.id);
	}
	
	public void testRelationship() {
		DiscoveryStringId radioactivity = new DiscoveryStringId("Radioactivity", StringID_CURIE);
		DiscoveryStringId relativity = new DiscoveryStringId("Relativity", StringID_EINSTEIN);
		DiscoveryStringId teslaCoil = new DiscoveryStringId("Tesla Coil", StringID_TESLA);
		
		pm.insert(radioactivity);
		pm.insert(relativity);
		pm.insert(teslaCoil);

		DiscoveryStringId relativity2 = pm.createQuery(DiscoveryStringId.class).filter("discoverer", StringID_EINSTEIN).get();
		assertTrue(relativity.name.equals(relativity2.name));

	}
	
	public void testMultipleKeys() {
		if(!supportsMultipleKeys()) return;
		
		MultipleKeys a = new MultipleKeys();
		a.id1 = "aid1";
		a.id2 = "aid2";
		a.name = "first";
		a.parent = null;
		pm.insert(a);

		MultipleKeys b = new MultipleKeys();
		b.id1 = "bid1";
		b.id2 = "bid2";
		b.name = "second";
		b.parent = null;
		pm.insert(b);
		
		b.parent = a;
		pm.update(b);
	}
	
	public void testDataTypesNull() {
		DataTypes dataTypes = new DataTypes();
		pm.insert(dataTypes);
		
		assertEqualsDataTypes(dataTypes, pm.createQuery(DataTypes.class).get());
	}
	
	public void testDataTypesNotNull() {
		char[] c = new char[501];
		Arrays.fill(c, 'x');
		
		DataTypes dataTypes = new DataTypes();
		dataTypes.typeByte = 1;
		dataTypes.typeShort = 2;
		dataTypes.typeInt = 3;
		dataTypes.typeLong = 4;
		dataTypes.typeFloat = 5;
		dataTypes.typeDouble = 6;
		dataTypes.typeDate = new Date();
		dataTypes.typeString = "hello";
		dataTypes.typeLargeString = new String(c);
		dataTypes.typeJson = map().put("foo", "bar");
		dataTypes.addresses = new ArrayList<Address>();
		dataTypes.addresses.add(new Address("Castellana", "Madrid"));
		dataTypes.addresses.add(new Address("Diagonal", "Barcelona"));
		dataTypes.contacts = new HashMap<String, Contact>();
		dataTypes.contacts.put("id1", new Contact("Somebody", Arrays.asList("foo", "bar")));
		
		dataTypes.shortShort = Short.MAX_VALUE;
		dataTypes.intInt = Integer.MAX_VALUE;
		dataTypes.longLong = Long.MAX_VALUE;
		dataTypes.boolBool = Boolean.TRUE;
		
		// Blob
		dataTypes.typeBlob = new byte[] { 
				(byte)0x01, (byte)0x02, (byte)0x03, (byte)0x04,
				(byte)0x10,	(byte)0X11, (byte)0xF0, (byte)0xF1, 
				(byte)0xF9,	(byte)0xFF };
		
		dataTypes.typeEnum = EnumLong.ALPHA;
		
		pm.insert(dataTypes);
		
		// to test that fields are read back correctly
		pm.createQuery(DataTypes.class).filter("id", dataTypes.id).get();
		
		DataTypes same = pm.createQuery(DataTypes.class).get();
		assertEqualsDataTypes(dataTypes, same);
	}
	
	
	
	public void testQueryDelete() {
		DiscoveryStringId radioactivity = new DiscoveryStringId("Radioactivity", StringID_CURIE);
		DiscoveryStringId relativity = new DiscoveryStringId("Relativity", StringID_EINSTEIN);
		DiscoveryStringId teslaCoil = new DiscoveryStringId("Tesla Coil", StringID_TESLA);
		
		pm.insert(radioactivity);
		pm.insert(relativity);
		pm.insert(teslaCoil);

		int n = pm.createQuery(DiscoveryStringId.class).delete();
		assertEquals(3, n);
		
		List<DiscoveryStringId> res = pm.createQuery(DiscoveryStringId.class).fetch();
		assertEquals(0, res.size());
	}
	
	public void testQueryDeleteFiltered() {
		DiscoveryStringId radioactivity = new DiscoveryStringId("Radioactivity", StringID_CURIE);
		DiscoveryStringId relativity = new DiscoveryStringId("Relativity", StringID_EINSTEIN);
		DiscoveryStringId foo = new DiscoveryStringId("Foo", StringID_EINSTEIN);
		DiscoveryStringId teslaCoil = new DiscoveryStringId("Tesla Coil", StringID_TESLA);
		
		pm.insert(radioactivity);
		pm.insert(relativity);
		pm.insert(foo);
		pm.insert(teslaCoil);

		int n = pm.createQuery(DiscoveryStringId.class).filter("discoverer", StringID_EINSTEIN).delete();
		assertEquals(2, n);

		List<DiscoveryStringId> res = pm.createQuery(DiscoveryStringId.class).order("name").fetch();
		assertEquals(2, res.size());
		assertEquals(radioactivity, res.get(0));
		assertEquals(teslaCoil, res.get(1));
	}
	
	public void testJoin() {
		DiscoveryStringId radioactivity = new DiscoveryStringId("Radioactivity", StringID_CURIE);
		DiscoveryStringId relativity = new DiscoveryStringId("Relativity", StringID_EINSTEIN);
		DiscoveryStringId foo = new DiscoveryStringId("Foo", StringID_EINSTEIN);
		DiscoveryStringId teslaCoil = new DiscoveryStringId("Tesla Coil", StringID_TESLA);
		
		pm.insert(radioactivity);
		pm.insert(relativity);
		pm.insert(foo);
		pm.insert(teslaCoil);
		
		List<DiscoveryStringId> res = pm.createQuery(DiscoveryStringId.class).join("discoverer").order("name").fetch();
		assertEquals(4, res.size());
		assertEquals(foo, res.get(0));
		assertEquals(radioactivity, res.get(1));
		assertEquals(relativity, res.get(2));
		assertEquals(teslaCoil, res.get(3));
		
		assertEquals(StringID_EINSTEIN, res.get(0).discoverer);
		assertEquals(StringID_CURIE, res.get(1).discoverer);
		assertEquals(StringID_EINSTEIN, res.get(2).discoverer);
		assertEquals(StringID_TESLA, res.get(3).discoverer);
	}
	
	public void testJoinSortFields() {
		DiscoveryStringId radioactivity = new DiscoveryStringId("Radioactivity", StringID_CURIE);
		DiscoveryStringId relativity = new DiscoveryStringId("Relativity", StringID_EINSTEIN);
		DiscoveryStringId foo = new DiscoveryStringId("Foo", StringID_EINSTEIN);
		DiscoveryStringId teslaCoil = new DiscoveryStringId("Tesla Coil", StringID_TESLA);
		
		pm.insert(radioactivity);
		pm.insert(relativity);
		pm.insert(foo);
		pm.insert(teslaCoil);
		
		List<DiscoveryStringId> res = pm.createQuery(DiscoveryStringId.class).join("discoverer", "firstName").order("name").fetch();
		assertEquals(4, res.size());
		assertEquals(foo, res.get(0));
		assertEquals(radioactivity, res.get(1));
		assertEquals(relativity, res.get(2));
		assertEquals(teslaCoil, res.get(3));
		
		assertEquals(StringID_EINSTEIN, res.get(0).discoverer);
		assertEquals(StringID_CURIE, res.get(1).discoverer);
		assertEquals(StringID_EINSTEIN, res.get(2).discoverer);
		assertEquals(StringID_TESLA, res.get(3).discoverer);
	}
	
	
	public void testJoinAnnotation() {
		Discovery4JoinStringId radioactivity = new Discovery4JoinStringId("Radioactivity", StringID_CURIE, StringID_TESLA);
		Discovery4JoinStringId relativity = new Discovery4JoinStringId("Relativity", StringID_EINSTEIN, StringID_TESLA);
		Discovery4JoinStringId foo = new Discovery4JoinStringId("Foo", StringID_EINSTEIN, StringID_EINSTEIN);
		Discovery4JoinStringId teslaCoil = new Discovery4JoinStringId("Tesla Coil", StringID_TESLA, StringID_CURIE);
		
		pm.insert(radioactivity);
		pm.insert(relativity);
		pm.insert(foo);
		pm.insert(teslaCoil);
		
		List<Discovery4JoinStringId> res = pm.createQuery(Discovery4JoinStringId.class).fetch();
		assertEquals(4, res.size());
		assertEquals(radioactivity, res.get(0));
		assertEquals(relativity, res.get(1));
		assertEquals(foo, res.get(2));
		assertEquals(teslaCoil, res.get(3));
		
		assertEquals(StringID_CURIE, res.get(0).discovererJoined);
		assertEquals(StringID_EINSTEIN, res.get(1).discovererJoined);
		assertEquals(StringID_EINSTEIN, res.get(2).discovererJoined);
		assertEquals(StringID_TESLA, res.get(3).discovererJoined);

		assertEquals(StringID_TESLA.id, res.get(0).discovererNotJoined.id);
		assertEquals(StringID_TESLA.id, res.get(1).discovererNotJoined.id);
		assertEquals(StringID_EINSTEIN.id, res.get(2).discovererNotJoined.id);
		assertEquals(StringID_CURIE.id, res.get(3).discovererNotJoined.id);
		
		assertTrue(res.get(0).discovererNotJoined.isOnlyIdFilled());
		assertTrue(res.get(1).discovererNotJoined.isOnlyIdFilled());
		assertTrue(res.get(2).discovererNotJoined.isOnlyIdFilled());
		assertTrue(res.get(3).discovererNotJoined.isOnlyIdFilled());
	}

	
	public void testFetchPrivateFields() {
		DiscoveryPrivate radioactivity = new DiscoveryPrivate(1L, "Radioactivity", LongAutoID_CURIE);
		DiscoveryPrivate relativity = new DiscoveryPrivate(2L, "Relativity", LongAutoID_EINSTEIN);
		DiscoveryPrivate foo = new DiscoveryPrivate(3L, "Foo", LongAutoID_EINSTEIN);
		DiscoveryPrivate teslaCoil = new DiscoveryPrivate(4L, "Tesla Coil", LongAutoID_TESLA);
		
		pm.insert(radioactivity);
		pm.insert(relativity);
		pm.insert(foo);
		pm.insert(teslaCoil);

		List<DiscoveryPrivate> res = pm.createQuery(DiscoveryPrivate.class).order("name").fetch();
		assertEquals(foo, res.get(0));
		assertEquals(radioactivity, res.get(1));
		assertEquals(relativity, res.get(2));
		assertEquals(teslaCoil, res.get(3));
	}
	

	
	/*
	
	
	
	
	
	
	private PersonUUID getPersonUUID(String id) {
		PersonUUID p = new PersonUUID();
		p.id = id;
		pm.get(p);
		return p;
	}

	private PersonLongAutoID getPersonLongAutoID(Long id) {
		PersonLongAutoID p = new PersonLongAutoID();
		p.id = id;
		pm.get(p);
		return p;
	}
	
	private PersonLongManualID getPersonLongManualID(Long id) {
		PersonLongManualID p = new PersonLongManualID();
		p.id = id;
		pm.get(p);
		return p;
	}

	private PersonStringID getPersonStringID(String id) {
		PersonStringID p = new PersonStringID();
		p.id = id;
		pm.get(p);
		return p;
	}

	private PersonUUID getByKeyPersonUUID(String id) {
		return pm.getByKey(PersonUUID.class, id);
	}

	private PersonLongAutoID getByKeyPersonLongAutoID(Long id) {
		return pm.getByKey(PersonLongAutoID.class, id);
	}
	
	private PersonLongManualID getByKeyPersonLongManualID(Long id) {
		return pm.getByKey(PersonLongManualID.class, id);
	}

	private PersonStringID getByKeyPersonStringID(String id) {
		return pm.getByKey(PersonStringID.class, id);
	}

	public void testFetchStringAutoInc() {
		PersonStringAutoIncID person = new PersonStringAutoIncID("TEST1", "TEST2", "TEST3", "TEST4", 123);
		
		pm.insert(person);
		
		List<PersonStringAutoIncID> l = pm.getByKeys(PersonStringAutoIncID.class, "TEST1");
		assertEquals(person, l.get(0));
	}
	
	public void testDumpQueryOption() {
		Query<PersonLongAutoID> query = pm.createQuery(PersonLongAutoID.class);
		
		QueryOption opt = query.option(QueryOptionPage.ID);
		Json dump = opt.dump();
		String str = JsonSerializer.serialize(dump).toString();
		assertNotNull(str);
		assertEquals("{\"value\": {\"pageType\": \"TEMPORARY\", \"state\": \"PASSIVE\", \"pageSize\": 0, \"type\": 1}, \"type\": \""+QueryOptionPage.class.getName()+"\"}", str);
	}
	
	public void testRestoreQueryOption() {
		QueryOption optRestored = (QueryOption)JsonSerializer.deserialize(QueryOption.class, Json.loads(
			"{\"type\":\""+QueryOptionPage.class.getName()+"\", \"value\": {\"pageType\": \"TEMPORARY\", \"state\": \"PASSIVE\", \"pageSize\": 0, \"type\": 1} }"
		));
		Query<PersonLongAutoID> query = pm.createQuery(PersonLongAutoID.class);
		
		QueryOption opt = query.option(QueryOptionPage.ID);
		
		assertEquals(opt, optRestored);
	}
	
	public void testDumpRestoreQueryFilterSimple() {
		Query<PersonLongAutoID> query = pm.createQuery(PersonLongAutoID.class).filter("firstName", "abcde");
		QueryFilterSimple qf = (QueryFilterSimple)query.getFilters().get(0);
		String str = JsonSerializer.serialize(qf).toString();
		assertNotNull(str);
		
		QueryFilterSimple qfRes = (QueryFilterSimple)JsonSerializer.deserialize(QueryFilter.class, Json.loads(str));
		assertNotNull(qfRes);
		assertEquals(qf.operator, qfRes.operator);
		assertEquals(qf.value, qfRes.value);
		assertEquals(qf.field.getName(), qfRes.field.getName());
	}
	
	public void testDumpRestoreQueryFilterSearch() {
		Query<PersonLongAutoID> query = pm.createQuery(PersonLongAutoID.class).search("test", "firstName", "lastName");
		QueryFilterSearch qf = (QueryFilterSearch)query.getFilters().get(0);
		String str = JsonSerializer.serialize(qf).toString();
		assertNotNull(str);
		
		QueryFilterSearch qfRes = (QueryFilterSearch)JsonSerializer.deserialize(QueryFilter.class, Json.loads(str));
		assertNotNull(qfRes);
		assertEquals(qf.match, qfRes.match);
		for(int i=0; i<qfRes.fields.length; i++){
			assertEquals(qf.fields[i], qfRes.fields[i]);
		}
	}
	
	public void testDumpRestoreQueryOrder() {
		Query<PersonLongAutoID> query = pm.createQuery(PersonLongAutoID.class).order("firstName");
		QueryOrder qo = (QueryOrder)query.getOrders().get(0);
		String str = JsonSerializer.serialize(qo).toString();
		assertNotNull(str);
		
		QueryOrder qoRes = (QueryOrder)JsonSerializer.deserialize(QueryOrder.class, Json.loads(str));
		assertNotNull(qoRes);
		assertEquals(qo.ascending, qoRes.ascending);
		assertEquals(qo.field.getName(), qoRes.field.getName());
	}
	
	public void testDumpRestoreQueryJoin() {
		Query<Discovery> query = pm.createQuery(Discovery.class).join("discoverer", "firstName");
		QueryJoin qj = (QueryJoin)query.getJoins().get(0);
		String str = JsonSerializer.serialize(qj).toString();
		assertNotNull(str);
		
		QueryJoin qjRes = (QueryJoin)JsonSerializer.deserialize(QueryJoin.class, Json.loads(str));
		assertNotNull(qjRes);
		assertEquals(qj.field.getName(), qjRes.field.getName());
		for(int i=0; i<qjRes.sortFields.length; i++){
			assertEquals(qj.sortFields[i], qjRes.sortFields[i]);
		}
	}
	
	public void testDumpRestoreQueryData() {
		Query<Discovery> query = 
			pm.createQuery(Discovery.class)
				.filter("name", "test").order("name").join("discoverer", "firstName");
		String str = JsonSerializer.serialize(query).toString();
		assertNotNull(str);
		
		Query<Discovery> qjRes = (Query<Discovery>)JsonSerializer.deserialize(BaseQuery.class, Json.loads(str));
		assertNotNull(qjRes);
		for(int i=0; i<qjRes.getFilters().size(); i++){
			assertEquals(query.getFilters().get(i), qjRes.getFilters().get(i));
		}
		for(int i=0; i<qjRes.getJoins().size(); i++){
			assertEquals(query.getJoins().get(i), qjRes.getJoins().get(i));
		}
		for(int i=0; i<qjRes.getOrders().size(); i++){
			assertEquals(query.getOrders().get(i), qjRes.getOrders().get(i));
		}
		for(int i=0; i<qjRes.getSearches().size(); i++){
			assertEquals(query.getSearches().get(i), qjRes.getSearches().get(i));
		}
	}
	
	
	
    public void testInsertObjectWithNullJoinObject() {
        Discovery4Join model = new Discovery4Join();
        model.discovererJoined = null; // explicitly set the join object to null

        pm.insert(model);

        Query<Discovery4Join> query = pm.createQuery(Discovery4Join.class).filter("id", model.id);
        Discovery4Join modelFromDatabase = pm.get(query);
        assertNull(modelFromDatabase.discovererJoined);
    }
    
    public void testInsertObjectWithDoubleNullJoinObject() {
        Discovery4Join2 model = new Discovery4Join2();
        model.discovererJoined = null; // explicitly set the join object to null
        model.discovererJoined2 = null; // explicitly set the join object to null

        pm.insert(model);

        Query<Discovery4Join2> query = pm.createQuery(Discovery4Join2.class).filter("id", model.id);
        Discovery4Join2 modelFromDatabase = pm.get(query);
        assertNull(modelFromDatabase.discovererJoined);
        assertNull(modelFromDatabase.discovererJoined2);
    }
    
	public void testJoinAnnotationDouble() {
		Discovery4Join2 radioactivity = new Discovery4Join2("Radioactivity", LongAutoID_CURIE, LongAutoID_TESLA);
		Discovery4Join2 relativity = new Discovery4Join2("Relativity", LongAutoID_EINSTEIN, LongAutoID_TESLA);
		Discovery4Join2 foo = new Discovery4Join2("Foo", LongAutoID_EINSTEIN, LongAutoID_EINSTEIN);
		Discovery4Join2 teslaCoil = new Discovery4Join2("Tesla Coil", LongAutoID_TESLA, LongAutoID_CURIE);
		
		pm.insert(radioactivity);
		pm.insert(relativity);
		pm.insert(foo);
		pm.insert(teslaCoil);
		
		List<Discovery4Join2> res = pm.createQuery(Discovery4Join2.class).fetch();
		assertEquals(4, res.size());
		assertEquals(radioactivity, res.get(0));
		assertEquals(relativity, res.get(1));
		assertEquals(foo, res.get(2));
		assertEquals(teslaCoil, res.get(3));
		
		assertEquals(LongAutoID_CURIE, res.get(0).discovererJoined);
		assertEquals(LongAutoID_EINSTEIN, res.get(1).discovererJoined);
		assertEquals(LongAutoID_EINSTEIN, res.get(2).discovererJoined);
		assertEquals(LongAutoID_TESLA, res.get(3).discovererJoined);

		assertEquals(LongAutoID_TESLA, res.get(0).discovererJoined2);
		assertEquals(LongAutoID_TESLA, res.get(1).discovererJoined2);
		assertEquals(LongAutoID_EINSTEIN, res.get(2).discovererJoined2);
		assertEquals(LongAutoID_CURIE, res.get(3).discovererJoined2);

	}
	

	
	
	
	
	
	
	public void testEmbeddedModel() {
		EmbeddedModel embed = new EmbeddedModel();
		embed.id = "embed";
		embed.alpha = "test";
		embed.beta = 123;
		pm.insert(embed);
		
		ContainerModel container = new ContainerModel();
		container.id = "container";
		container.embed = embed;
		pm.insert(container);

		ContainerModel afterContainer = pm.getByKey(ContainerModel.class, container.id);
		assertNotNull(afterContainer);
		assertEquals(container.id, afterContainer.id);
		assertNotNull(afterContainer.embed);
		assertEquals(embed.id, afterContainer.embed.id);
		assertEquals(null, afterContainer.embed.alpha);
		assertEquals(embed.beta, afterContainer.embed.beta);
	}

	public void testNoColumn() {
		DiscoveryNoColumn radioactivity = new DiscoveryNoColumn("Radioactivity", LongAutoID_CURIE, LongAutoID_TESLA);
		DiscoveryNoColumn relativity = new DiscoveryNoColumn("Relativity", LongAutoID_EINSTEIN, LongAutoID_TESLA);
		DiscoveryNoColumn foo = new DiscoveryNoColumn("Foo", LongAutoID_EINSTEIN, LongAutoID_EINSTEIN);
		DiscoveryNoColumn teslaCoil = new DiscoveryNoColumn("Tesla Coil", LongAutoID_TESLA, LongAutoID_CURIE);
		
		pm.insert(radioactivity);
		pm.insert(relativity);
		pm.insert(foo);
		pm.insert(teslaCoil);
		
		List<DiscoveryNoColumn> res = pm.createQuery(DiscoveryNoColumn.class).fetch();
		assertEquals(4, res.size());
		assertEquals(radioactivity, res.get(0));
		assertEquals(relativity, res.get(1));
		assertEquals(foo, res.get(2));
		assertEquals(teslaCoil, res.get(3));
		
		assertEquals(LongAutoID_CURIE, res.get(0).discovererJoined);
		assertEquals(LongAutoID_EINSTEIN, res.get(1).discovererJoined);
		assertEquals(LongAutoID_EINSTEIN, res.get(2).discovererJoined);
		assertEquals(LongAutoID_TESLA, res.get(3).discovererJoined);

		assertEquals(LongAutoID_TESLA.id, res.get(0).discovererNotJoined.id);
		assertEquals(LongAutoID_TESLA.id, res.get(1).discovererNotJoined.id);
		assertEquals(LongAutoID_EINSTEIN.id, res.get(2).discovererNotJoined.id);
		assertEquals(LongAutoID_CURIE.id, res.get(3).discovererNotJoined.id);
		
		assertTrue(res.get(0).discovererNotJoined.isOnlyIdFilled());
		assertTrue(res.get(1).discovererNotJoined.isOnlyIdFilled());
		assertTrue(res.get(2).discovererNotJoined.isOnlyIdFilled());
		assertTrue(res.get(3).discovererNotJoined.isOnlyIdFilled());
	}

	public void testNoColumnMultipleKeys() {
		if(!supportsMultipleKeys()) return;
		
		MultipleKeys mk1 = new MultipleKeys();
		mk1.id1 = "aid1";
		mk1.id2 = "aid2";
		mk1.name = "first";
		mk1.parent = null;
		pm.insert(mk1);

		MultipleKeys mk2 = new MultipleKeys();
		mk2.id1 = "bid1";
		mk2.id2 = "bid2";
		mk2.name = "second";
		mk2.parent = null;
		pm.insert(mk2);
		
		mk2.parent = mk1;
		pm.update(mk2);
		
		DiscoveryNoColumnMultipleKeys disc = new DiscoveryNoColumnMultipleKeys("disc1", mk1, mk2);
		pm.insert(disc);
		
		DiscoveryNoColumnMultipleKeys afterDisc = pm.getByKey(DiscoveryNoColumnMultipleKeys.class, disc.id);
		assertNotNull(afterDisc);
		assertEquals("disc1", afterDisc.name);
		assertEquals(mk1.id1, afterDisc.mk1.id1);
		assertEquals(mk1.id2, afterDisc.mk1.id2);
		assertEquals(mk2.id1, afterDisc.mk2.id1);
		assertEquals(mk2.id2, afterDisc.mk2.id2);
	}
	
	public void testLifeCycleGet(){
		PersistenceManagerLifeCycleWrapper pml = new PersistenceManagerLifeCycleWrapper(pm);
		
		DiscoveryLifeCycle before = new DiscoveryLifeCycle("Radioactivity", LongAutoID_CURIE);
		pm.insert(before);
		
		lifeCyclePhase = "";
		DiscoveryLifeCycle after = new DiscoveryLifeCycle();
		after.id = before.id;
		pml.get(after);
		
		assertEquals(LifeCyclePhase.PRE_FETCH.toString()+" "+LifeCyclePhase.POST_FETCH.toString()+" ", lifeCyclePhase);
	}
	
	public void testLifeCycleGetMultiAndLifeCycleInjection(){
		PersistenceManagerLifeCycleWrapper pml = new PersistenceManagerLifeCycleWrapper(pm);
		
		DiscoveryLifeCycleMulti before = new DiscoveryLifeCycleMulti("Radioactivity", LongAutoID_CURIE);
		pm.insert(before);
		
		lifeCyclePhase = "";
		DiscoveryLifeCycleMulti after = new DiscoveryLifeCycleMulti();
		after.id = before.id;
		pml.get(after);
		
		assertEquals(LifeCyclePhase.PRE_FETCH.toString()+" "+LifeCyclePhase.POST_FETCH.toString()+" ", lifeCyclePhase);
	}
	
	public void testLifeCycleInsert(){
		PersistenceManagerLifeCycleWrapper pml = new PersistenceManagerLifeCycleWrapper(pm);
		
		lifeCyclePhase = "";
		DiscoveryLifeCycle before = new DiscoveryLifeCycle("Radioactivity", LongAutoID_CURIE);
		pml.insert(before);
		
		assertEquals(LifeCyclePhase.PRE_INSERT.toString()+" "+LifeCyclePhase.POST_INSERT.toString()+" ", lifeCyclePhase);
	}
	
	public void testLifeCycleDelete(){
		PersistenceManagerLifeCycleWrapper pml = new PersistenceManagerLifeCycleWrapper(pm);
		
		lifeCyclePhase = "";
		DiscoveryLifeCycle before = new DiscoveryLifeCycle("Radioactivity", LongAutoID_CURIE);
		pm.insert(before);
		
		pml.delete(before);
		
		assertEquals(LifeCyclePhase.PRE_DELETE.toString()+" "+LifeCyclePhase.POST_DELETE.toString()+" ", lifeCyclePhase);
	}
	
	public void testLifeCycleUpdate(){
		PersistenceManagerLifeCycleWrapper pml = new PersistenceManagerLifeCycleWrapper(pm);
		
		DiscoveryLifeCycle before = new DiscoveryLifeCycle("Radioactivity", LongAutoID_CURIE);
		pm.insert(before);
		
		lifeCyclePhase = "";
		before.name = "Radioactivity_UPD";
		pml.update(before);
		
		assertEquals(LifeCyclePhase.PRE_UPDATE.toString()+" "+LifeCyclePhase.POST_UPDATE.toString()+" ", lifeCyclePhase);
	}
	
	public void testLifeCycleSave(){
		PersistenceManagerLifeCycleWrapper pml = new PersistenceManagerLifeCycleWrapper(pm);
		
		lifeCyclePhase = "";
		DiscoveryLifeCycle before = new DiscoveryLifeCycle("Radioactivity", LongAutoID_CURIE);
		pml.save(before);
		
		assertEquals(LifeCyclePhase.PRE_SAVE.toString()+" "+LifeCyclePhase.POST_SAVE.toString()+" ", lifeCyclePhase);
	}
	
	public void testSerializeEmbeddedModel() {
		EmbeddedModel embed = new EmbeddedModel();
		embed.id = "embed";
		embed.alpha = "test";
		embed.beta = 123;
		pm.insert(embed);

		EmbeddedSubModel subEmbed = new EmbeddedSubModel();
		subEmbed.id = "subembed";
		subEmbed.parent = embed;
		
		ContainerModel container = new ContainerModel();
		container.id = "container";
		container.embed = embed;
		pm.insert(container);
		
		ContainerModel afterContainer = pm.getByKey(ContainerModel.class, container.id);
		assertNotNull(afterContainer);
		assertEquals(container.id, afterContainer.id);
		assertNotNull(afterContainer.embed);
		assertEquals(embed.id, afterContainer.embed.id);
		assertEquals(null, afterContainer.embed.alpha);
		assertEquals(embed.beta, afterContainer.embed.beta);
		assertEquals(null, afterContainer.embed.subs);
	}
	
	
	
	*/
}
