/*
 * Copyright 2011 Pascal Voitot <pascal.voitot.dev@gmail.com>
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

package siena.samples.relations.referenced.unowned.one2one2ways;

import siena.Generator;
import siena.Id;
import siena.Model;
import siena.Query;
import siena.Table;
import siena.core.Referenced;
import siena.core.batch.Batch;

@Table("sample_unowned_one2one2ways_satellite")
public class Satellite extends Model{

	@Id(Generator.AUTO_INCREMENT)
	public Long id;
	
	public String name;
	
	@Referenced
	public Planet planet;

	public static Query<Satellite> all() {
		return Model.all(Satellite.class);
	}
	
	public static Batch<Satellite> batch() {
		return Model.batch(Satellite.class);
	}
	
	public Satellite() {
	}

	public Satellite(String name) {
		this.name = name;
	}
	
	public static Satellite getByName(String name){
		return all().filter("name", name).get();
	}

	public String toString() {
		return "id: "+id+" - name: "+name+" - planet:"+planet;
	}
}
