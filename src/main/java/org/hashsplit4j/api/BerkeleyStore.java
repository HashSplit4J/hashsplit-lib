package org.hashsplit4j.api;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;

public class BerkeleyStore implements BlobStore {
  
  private static final long CACHE_SIZE = 20 * 1024 * 1024;
  
  private static final String CHARSET_UTF = "UTF-8";
  
  private Map<String, byte[]> mapOfChunks = new HashMap<String, byte[]>();
  
  Environment env;
  
  Database db;
  
  // Default folder for stored files
  String folder;

  // String used to identify the database
  String dbName;
  
  public BerkeleyStore(String folder, String dbName) {
    this.folder = folder;
    this.dbName = dbName;
  }

  @Override
  public void setBlob(String hash, byte[] bytes) {
    if (hash == null || bytes == null)
      throw new RuntimeException("Key and value can not be null for setBlob() function");
    
    env = createDBEnvironment();
    db = openDatabase();
    
    try {
      DatabaseEntry key = new DatabaseEntry(hash.getBytes(CHARSET_UTF));
      DatabaseEntry data = new DatabaseEntry(bytes);
      db.putNoOverwrite(null, key, data);
      mapOfChunks.put(hash, bytes);
    } catch (UnsupportedEncodingException e) {
      //TODO: Edit here!!!
    } finally {
      close();
    }
  }

  @Override
  public byte[] getBlob(String hash) {
    if (hash == null)
      throw new RuntimeException("Key can not be null for setBlob() function");
    
    this.env = createDBEnvironment();
    this.db = openDatabase();
    
    try {
      DatabaseEntry search = new DatabaseEntry();
      db.get(null, new DatabaseEntry(hash.getBytes(CHARSET_UTF)), search, LockMode.DEFAULT);
      return search.getData();
    } catch (UnsupportedEncodingException e) {
      // TODO: Edit here!!!
    } finally {
      close();
    }
    
    return null;
  }

  @Override
  public boolean hasBlob(String hash) {
    return mapOfChunks.containsKey(hash);
  }

  /**
   * Inits the databased environment used for all databases.
   * 
   * @return environment
   */
  private Environment createDBEnvironment() {
    File dbDir = new File(folder);
    if (!dbDir.exists())
      if (!dbDir.mkdirs())
        throw new RuntimeException("The directory " + folder + " does not exist.");
    
    EnvironmentConfig envCfg = new EnvironmentConfig();
    envCfg.setDurability(Durability.COMMIT_SYNC);
    envCfg.setAllowCreate(true);
    envCfg.setSharedCache(true);
    envCfg.setCacheSize(CACHE_SIZE);
    return new Environment(dbDir, envCfg);
  }
  
  private Database openDatabase() {
    DatabaseConfig dbCfg = new DatabaseConfig();
    dbCfg.setAllowCreate(true);
    dbCfg.setSortedDuplicates(false);
    dbCfg.setOverrideDuplicateComparator(false);
    return env.openDatabase(null, dbName, dbCfg);
  }
  
  private void close() {
    if (db != null)
      db.close();
    
    if (env != null) {
      env.cleanLog();
      env.close();
    }
  }
}
