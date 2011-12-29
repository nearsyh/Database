#ifndef MYDB_H
#define MYDB_H

#include "../bdb/db_cxx.h"
#include <string>
#include <vector>
#include <map>
#include <iostream>
using namespace std;

#define MAX 1000000000


class MyDb : public Db {
public:
	using Db::close;
	// Constructor requires a path to the database,
    // and a database name.
    MyDb(std::string &path, const std::string &dbName,
         bool isSecondary = false, bool isInt = false);

    // Our destructor just calls our private close method.
    ~MyDb() { close(); }
    
    // Open Data Base
    void open(int i) {
        if(i == 0)
            Db::open(NULL, dbFileName_.c_str(), NULL, DB_BTREE, cFlags_, 0);
        else
            Db::open(NULL, dbFileName_.c_str(), NULL, DB_HASH, cFlags_, 0);
    }

    void setColumnNo(int c) {
        columnNo = c;
    }

    unsigned int getColumnNo() {
        return columnNo;
    }

    bool isSecond() {
        return isSecondary;
    }

    bool isInt() {
        return isInteger;
    }

    int associate(const string& columnName, DbTxn *txnid, Db *secondary,
            int (*callback)(Db *secondary,const Dbt *key, const Dbt *data, Dbt *result),
            u_int32_t flags) {
        second[columnName] = (MyDb*)secondary;
        Db::associate(txnid, secondary, callback, flags);
    }

    int put(DbTxn *txnid, Dbt *key, Dbt *data, u_int32_t flags) {
        count ++;
        return Db::put(txnid, key, data, flags);
    }

    MyDb* get_second_database(char *columnName) {
        return second[string(columnName)];
    }
    std::string dbFileName_;

    inline unsigned int getCount() {return count;}
private:
    u_int32_t cFlags_;
    unsigned int count;
    int columnNo;
    bool isSecondary;
	bool isInteger;
    map<string, MyDb* > second;

    // Make sure the default constructor is private
    // We don't want it used.
    MyDb() : Db::Db(NULL, 0) {count = 0;}

    // We put our database close activity here.
    // This is called from our destructor. In
    // a more complicated example, we might want
    // to make this method public, but a private
    // method is more appropriate for this example.
    void close();
};

int compare_dbt(DB *d, const DBT *a, const DBT*b);

size_t compare_prefix(DB *d, const DBT *a, const DBT *b);

#endif
