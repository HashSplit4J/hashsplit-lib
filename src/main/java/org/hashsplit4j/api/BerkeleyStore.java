package org.hashsplit4j.api;

import java.io.File;
import java.nio.charset.Charset;

import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.LockMode;

public class BerkeleyStore implements BlobStore {
  
  private static final Charset CHARSET_UTF = Charset.forName("UTF-8");
  
  private Environment env;
  
  private Database db;
  
  /**
   * Default folder for stored files
   */
  private final File dbDir;

  /**
   * String used to identify the database
   */
  private final String dbName;
  
  private final long cacheSize;
  
  public BerkeleyStore(File dbDir, String dbName, long cacheSize) {
    this.dbDir = dbDir;
    this.dbName = dbName;
    this.cacheSize = cacheSize;
    this.env = createDBEnvironment();
    this.db = openDatabase();
  }

  @Override
  public void setBlob(String hash, byte[] bytes) {
    if (hash == null || bytes == null)
      throw new RuntimeException("Key and value can not be null for setBlob() function");
    
    DatabaseEntry key = new DatabaseEntry(hash.getBytes(CHARSET_UTF));
    DatabaseEntry data = new DatabaseEntry(bytes);
    db.putNoOverwrite(null, key, data);
  }

  @Override
  public byte[] getBlob(String hash) {
    if (hash == null)
      throw new RuntimeException("Key can not be null for setBlob() function");
    
    DatabaseEntry search = new DatabaseEntry();
    db.get(null, new DatabaseEntry(hash.getBytes(CHARSET_UTF)), search, LockMode.DEFAULT);
    return search.getData();
  }

  @Override
  public boolean hasBlob(String hash) {
    byte[] bytes = getBlob(hash);
    if (bytes != null && bytes.length >= 0)
      return true;
    
    return false;
  }
  
  public void close() {
    if (db != null) {
      db.close();
    }
    
    if (env != null) {
      env.cleanLog();
      env.close();
    }
  }

  /**
   * Inits the databased environment used for all databases.
   * 
   * @return environment
   */
  private Environment createDBEnvironment() {
    if (!dbDir.exists())
      if (!dbDir.mkdirs())
        throw new RuntimeException("The directory " + dbDir + " does not exist.");
    
    EnvironmentConfig envCfg = new EnvironmentConfig();
    envCfg.setAllowCreate(true);
    envCfg.setSharedCache(true);
    envCfg.setCacheSize(cacheSize);
    envCfg.setDurability(Durability.COMMIT_SYNC);
    return new Environment(dbDir, envCfg);
  }
  
  private Database openDatabase() {
    DatabaseConfig dbCfg = new DatabaseConfig();
    dbCfg.setAllowCreate(true);
    dbCfg.setSortedDuplicates(false);
    dbCfg.setOverrideDuplicateComparator(false);
    return env.openDatabase(null, dbName, dbCfg);
  }
}
