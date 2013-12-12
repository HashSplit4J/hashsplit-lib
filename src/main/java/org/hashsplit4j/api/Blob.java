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

/**
 * Blob entity is used for storage in the Berkeley DB
 *        e.g: Blob {"e111885f8b7797884299e513ace4b8174a6e25fa", "e11", 
 *                        "e11188", "Oracle Berkeley DB Java Edition"}
 * 
 * @author <a href="mailto:sondn@exoplatform.com">Ngoc Son Dang</a>
 * @version Blob.java Dec 11, 2013
 */
@Entity
public class Blob {

  @PrimaryKey
  private String hash;
  
  @SecondaryKey(relate=MANY_TO_ONE)
  private String group;
  
  @SecondaryKey(relate=MANY_TO_ONE)
  private String subGroup;
  
  private String contents;

  /**
   * For deserialization
   */
  private Blob() {}

  public Blob(String hash, String group, String subGroup, String contents) {
    this.hash = hash;
    this.group = group;
    this.subGroup = subGroup;
    this.contents = contents;
  }

  public String getHash() {
    return hash;
  }

  public String getGroup() {
    return group;
  }
  
  public String getSubGroup() {
    return subGroup;
  }

  public String getContents() {
    return contents;
  }
}
