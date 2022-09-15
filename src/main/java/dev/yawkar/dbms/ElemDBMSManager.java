package dev.yawkar.dbms;

import dev.yawkar.dbms.db.Database;
import dev.yawkar.dbms.db.FileDatabase;
import dev.yawkar.dbms.exception.CannotPerformOperationException;
import dev.yawkar.dbms.exception.DatabaseFileAlreadyExistsException;
import dev.yawkar.dbms.exception.DatabaseFileNotFoundException;
import dev.yawkar.dbms.exception.UnknownDatabaseTypeUriException;

import java.io.File;
import java.io.IOException;

public class ElemDBMSManager implements DBManager {

    @Override
    public Database getDatabase(String uri) {
        if (uri.startsWith("file:")) {
            File dbFile = new File(uri.substring(5));
            if (!dbFile.isFile()) {
                throw new DatabaseFileNotFoundException(dbFile.getName());
            }
            return new FileDatabase(uri, dbFile);
        } else {
            throw new UnknownDatabaseTypeUriException(uri);
        }
    }

    @Override
    public Database createDatabase(String uri) {
        if (uri.startsWith("file:")) {
            File dbFile = new File(uri.substring(5));
            if (dbFile.isFile()) {
                throw new DatabaseFileAlreadyExistsException(dbFile.getName());
            }
            try {
                dbFile.createNewFile();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return new FileDatabase(uri, dbFile);
        } else {
            throw new UnknownDatabaseTypeUriException(uri);
        }
    }

    @Override
    public void dropDatabase(String uri) {
        if (uri.startsWith("file:")) {
            File dbFile = new File(uri.substring(5));
            if (dbFile.exists()) {
                if (!dbFile.delete()) {
                    throw new CannotPerformOperationException("Cannot delete db file '%s'".formatted(dbFile.getName()));
                }
            } else {
                throw new DatabaseFileNotFoundException(dbFile.getName());
            }
        } else {
            throw new UnknownDatabaseTypeUriException(uri);
        }
    }

    @Override
    public void dropDatabase(Database database) {
        if (database.getUri().startsWith("file:")) {
            File dbFile = new File(database.getUri().substring(5));
            if (dbFile.exists()) {
                if (!dbFile.delete()) {
                    throw new CannotPerformOperationException("Cannot delete db file '%s'".formatted(dbFile.getName()));
                }
            } else {
                throw new DatabaseFileNotFoundException(dbFile.getName());
            }
        } else {
            throw new UnknownDatabaseTypeUriException(database.getUri());
        }
    }
}
