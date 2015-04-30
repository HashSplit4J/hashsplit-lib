/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hashsplit4j.api;

import com.sleepycat.persist.model.Entity;
import com.sleepycat.persist.model.PrimaryKey;
import static com.sleepycat.persist.model.Relationship.MANY_TO_ONE;
import com.sleepycat.persist.model.SecondaryKey;
import java.io.Serializable;
import java.util.List;

/**
 *
 * @author dylan
 */
@Entity
public class Hash implements Serializable{

    @PrimaryKey
    private String hash;
    private String group;

    @SecondaryKey(relate = MANY_TO_ONE)
    private String subGroup;

    private List<String> hashes;
    private long actualContentLength;

    private Hash() {
    }

    public Hash(String hash, String group, String subGroup, List<String> hashes, long actualContentLength) {
        this.hash = hash;
        this.group = group;
        this.subGroup = subGroup;
        this.hashes = hashes;
        this.actualContentLength = actualContentLength;
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

    public List<String> getHashes() {
        return this.hashes;
    }

    public long getActualContentLength() {
        return this.actualContentLength;
    }

}
