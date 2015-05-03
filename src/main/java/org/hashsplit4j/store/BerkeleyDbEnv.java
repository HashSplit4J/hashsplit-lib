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
package org.hashsplit4j.store;

import java.io.File;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.persist.EntityStore;
import com.sleepycat.persist.StoreConfig;
import java.util.Date;

public class BerkeleyDbEnv {

    private Environment env;
    private EntityStore store;
    private Date lastCommit;

    /**
     * Our constructor does nothing
     */
    public BerkeleyDbEnv() {
    }

    /**
     * Setup a Berkeley DB engine environment, and reload some blob records
     *
     * @param envHome home directory
     * @param readOnly
     * @throws DatabaseException
     */
    public void openEnv(File envHome, boolean readOnly) throws DatabaseException {
        if (!envHome.exists()) {
            if (!envHome.mkdirs()) {
                throw new RuntimeException("The directory " + envHome + " does not exist");
            }
        }

        EnvironmentConfig envConfig = new EnvironmentConfig()
                .setReadOnly(readOnly)
                .setTransactional(true)
                .setSharedCache(true)
                .setAllowCreate(!readOnly);
        envConfig.setTxnNoSyncVoid(true);

        StoreConfig storeConfig = new StoreConfig()
                .setReadOnly(readOnly)
                .setTransactional(true)
                .setAllowCreate(!readOnly);

        // If the environment is opened for write, then we want to be
        // able to create the environment and entity store if
        // they do not exist.

        // Open the environment and entity store
        env = new Environment(envHome, envConfig);
        store = new EntityStore(env, envHome.getName(), storeConfig);
    }

    /**
     * Get a handle to the entity store
     *
     * @return a entity store
     */
    public EntityStore getEntityStore() {
        return store;
    }

    /**
     * Get a handle to the environment
     *
     * @return a environment
     */
    public Environment getEnv() {
        return env;
    }

    /**
     * Close the store and environment
     */
    public void closeEnv() {
        if (store != null) {
            try {
                store.close();
            } catch (DatabaseException dbe) {
                System.err.println("Error closing store " + dbe.getMessage());
            }
        }

        if (env != null) {
            try {
                env.cleanLog();
                env.close();
            } catch (DatabaseException dbe) {
                System.err.println("Error closing environment " + dbe.getMessage());
            }
        }
    }

    /**
     * Remove Berkeley DB File
     *
     * @param envHome
     */
    public void removeDbFiles(File envHome) {
        for (File file : envHome.listFiles()) {
            if (!file.delete()) {
                System.err.println("Could not delete " + file.getAbsolutePath());
            }
        }

        if (!envHome.delete()) {
            System.err.println("Failed to delete berkeley-db directory");
        }
    }
}
