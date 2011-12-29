/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2004, 2011 Oracle and/or its affiliates.  All rights reserved.
 *
 * $Id$ 
 * B Tree
 */

#include <cstring>
#include "MyDb.h"

// File: MyDb.cpp

// Class constructor. Requires a path to the location
// where the database is located, and a database name
MyDb::MyDb(std::string &path, const std::string &dbName,
           bool isSecondary, bool isInt)
    : Db::Db(NULL, 0),            // Instantiate Db object
      dbFileName_(path + dbName), // Database file name
      cFlags_(DB_CREATE),         // If the database doesn't yet exist,
      count(0),                   // allow it to be created.
      isSecondary(isSecondary),
	  isInteger(isInt)
{
    try
    {
        // Redirect debugging information to std::cerr
        set_error_stream(&std::cerr);

        // If this is a secondary database, support
        // sorted duplicates
        if (isSecondary)
            set_flags(DB_DUPSORT);

    }
    // DbException is not a subclass of std::exception, so we
    // need to catch them both.
    catch(DbException &e)
    {
        std::cerr << "Error opening database: " << dbFileName_ << "\n";
        std::cerr << e.what() << std::endl;
    }
    catch(std::exception &e)
    {
        std::cerr << "Error opening database: " << dbFileName_ << "\n";
        std::cerr << e.what() << std::endl;
    }
}

// Private member used to close a database. Called from the class
// destructor.
void
MyDb::close()
{
    // Close the db
    try
    {
        if(!isSecondary) {
            map<string, MyDb* >::iterator iter = second.begin();
            for( ; iter != second.end(); iter ++)
                iter->second->close();
        }
        close(0);
        std::cout << "Database " << dbFileName_
                  << " is closed." << std::endl;
    }
    catch(DbException &e)
    {
            std::cerr << "Error closing database: " << dbFileName_ << "\n";
            std::cerr << e.what() << std::endl;
    }
    catch(std::exception &e)
    {
        std::cerr << "Error closing database: " << dbFileName_ << "\n";
        std::cerr << e.what() << std::endl;
    }
}

int compare_dbt(DB *d, const DBT *at, const DBT* bt) {
	MyDb *dbp = (MyDb *)(Db::get_Db(d));
    const Dbt *a = Dbt::get_const_Dbt(at);
    const Dbt *b = Dbt::get_const_Dbt(bt);
	if(dbp->isInt()) {
		unsigned int ai, bi;
		memcpy(&ai, a->get_data(), sizeof(unsigned int));
		memcpy(&bi, b->get_data(), sizeof(unsigned int));
		return (ai - bi);
	} else {
		char *as = new char[a->get_size()], *bs = new char[b->get_size()];
		memcpy(as, a->get_data(), a->get_size());
		memcpy(bs, b->get_data(), b->get_size());
	    //cout << as << " " << bs << endl;
		//cout << strlen(as) << "--" << strlen(bs) << endl;
		return strcmp(as, bs);
	}
}

size_t compare_prefix(DB *d, const DBT *at, const DBT *bt) {
	MyDb *dbp = (MyDb *)(Db::get_Db(d));
    const Dbt *a = Dbt::get_const_Dbt(at);
    const Dbt *b = Dbt::get_const_Dbt(bt);
	size_t cnt, len;
    size_t a_size = a->get_size(), b_size = b->get_size();
	char *p1, *p2;
	cnt = 0;
	len = a_size > b_size ? b_size : a_size;
	if(dbp->isInt())
		return 1;
	for (p1 = (char*)a->get_data(), p2 = (char*)b->get_data(); len--; cnt)
		if (p1[cnt] != p2[cnt])
			return (cnt+1);
	if (a_size < b_size)
		return (a_size + 1);
	if (b_size < a_size)
		return (b_size + 1);
	return (b_size);
}

