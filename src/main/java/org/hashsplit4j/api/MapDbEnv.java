/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.hashsplit4j.api;

import java.io.File;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;

/**
 *
 * @author dylan
 * @param <K>
 * @param <V>
 */
public class MapDbEnv<K, V> {

    private final File envHome;
    private DB db;
    HTreeMap<K, V> map;

    public MapDbEnv(File envHome) {
        this.envHome = envHome;
    }

    public void init(String dbName) {
        init(null, dbName);
    }

    public void init(byte[] password, String dbName) {
        DBMaker dbMaker = DBMaker.newFileDB(envHome)
                .closeOnJvmShutdown()
                .transactionDisable()
                .asyncWriteEnable()
                .compressionEnable();
        if (password != null) {
            dbMaker.encryptionEnable(password);
        }

        this.db = dbMaker.make();
        this.map = this.db.getHashMap(dbName);
    }

    public DB getDB() {
        return this.db;
    }

    public void add(K key, V value) {
        this.map.put(key, value);
    }

    public V get(K key) {
        V val = this.map.get(key);
        return val;
    }

    public boolean hasHash(K key) {
        return this.map.containsKey(key);
    }
}
