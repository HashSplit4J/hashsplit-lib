/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hashsplit4j.api;

import com.sleepycat.persist.model.Persistent;
import com.sleepycat.persist.model.PrimaryKey;
import java.io.Serializable;

/**
 *
 * @author dylan
 */
@Persistent
public class HashPersistant implements Serializable{

    @PrimaryKey
    public String hash;
    public String group;
}
