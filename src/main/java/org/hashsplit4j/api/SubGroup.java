/*
 * Copyright (C) McEvoy Software Ltd
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.hashsplit4j.api;

import static com.sleepycat.persist.model.Relationship.MANY_TO_ONE;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import com.sleepycat.persist.model.SecondaryKey;

@Entity
public class SubGroup {

	@PrimaryKey
	private String name;
	
	@SecondaryKey(relate = MANY_TO_ONE)
	private String parent;
	
	private String contentHash;
	
	@SecondaryKey(relate = MANY_TO_ONE)
    private String status; 	// Current status of group (include root group or sub group)
    						// INVALID	: Status is missing hash
    						// VALID	: status is valid 

	/**
	 * Needed for deserialization
	 */
	private SubGroup() {}
	
	public SubGroup(String name, String parent, String contentHash, String status) {
		this.name = name;
		this.parent = parent;
		this.contentHash = contentHash;
		this.status = status;
	}

	public String getName() {
		return name;
	}

	public String getParent() {
		return parent;
	}

	public String getContentHash() {
		return contentHash;
	}

	public String getStatus() {
		return status;
	}
}
