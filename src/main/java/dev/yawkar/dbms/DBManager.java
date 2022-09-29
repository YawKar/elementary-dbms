package dev.yawkar.dbms;

import dev.yawkar.dbms.db.Database;

public interface DBManager {

    Database getDatabase(String uri);
    Database createDatabase(String uri);
    void dropDatabase(String uri);
    void dropDatabase(Database database);
    void dumpDatabase(Database database);
}
