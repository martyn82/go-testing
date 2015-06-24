package db

import (
    "errors"
    "database/sql"
    "github.com/mattn/go-sqlite3"
    "io/ioutil"
    "os"
    "path"
)

/* Executes a query on the database */
func QueryDb(query string, dbFile string) {
    sqlite3.Version()

    var err error
    var db *sql.DB

    if db, err = sql.Open("sqlite3", dbFile); err != nil {
        panic(err)
    }

    defer db.Close()
    if _, err = db.Exec(query); err != nil {
        panic(err)
    }
}

/* Removes the db file as tear down */
func RemoveDbFile(dbFile string) {
    os.Remove(dbFile)
}

/* Setup Db */
func SetupDb(dbFile string, schemaPath string) (int, error) {
    sqlite3.Version()

    var db *sql.DB
    var err error
    var filesImported int

    if db, err = sql.Open("sqlite3", dbFile); err == nil {
        defer db.Close()
        filesImported, err = importSchema(schemaPath, db)
    }

    return filesImported, err
}

/* Imports the database schema into the database specified */
func importSchema(schemaPath string, db *sql.DB) (int, error) {
    var err error
    var infos []os.FileInfo

    if infos, err = ioutil.ReadDir(schemaPath); err != nil {
        return 0, err
    }

    filesImported := 0

    for _, info := range infos {
        if info.IsDir() || path.Ext(info.Name()) != ".sql" {
            continue
        }

        var contents []byte

        if contents, err = ioutil.ReadFile(path.Join(schemaPath, info.Name())); err != nil {
            return filesImported, err
        }

        var stmt *sql.Stmt

        if stmt, err = db.Prepare(string(contents)); err != nil {
            return filesImported, err
        }

        if _, err = stmt.Exec(); err != nil {
            return filesImported, err
        }

        filesImported++
    }

    if filesImported == 0 {
        err = errors.New("No schema files were imported.")
    }

    return filesImported, err
}
