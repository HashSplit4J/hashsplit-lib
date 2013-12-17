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

/**
 * The Status entity class and this class is used for determine 
 * a root group and sub group is missing hash or not?
 * 
 * Have two type of entity:
 * 		- INVALID	: Root group or Sub group is missing hash
 * 		- VALID		: Current hash is valid
 * 
 * @author sondn
 */
public class Status {

	public static final String INVALID = "INVALID";
	public static final String VALID = "VALID";
}
