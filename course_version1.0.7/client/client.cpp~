#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <map>
#include <cassert>
#include <set>

#include "MyDb.h"
#include "MyCondition.h"
#include "Record.h"
#include "../include/client.h"
#include "util/tokenize.h"
#include "util/split_csv.h"

using namespace std;

map<string, MyDb* > table2data;
map<string, string> col2table;
map<string, int> col2no;
set<string> indexSet;
vector<string> tableName;

map<string, vector<string> > table2name;
map<string, vector<string> > table2type;
map<string, vector<string> > table2pkey;
vector<string> result;

int get_data(Db *t, const Dbt *pkey, const Dbt *pdata, Dbt *skey);

void done(const vector<string>& table, const map<string, int>& m,
	int depth, vector<string>& row)
{
	FILE *fin;
	char buf[65536];
	vector<string> column_name, token;
	string str;
	int i;

	if (depth == table.size()) {
		str = row[0];
		for (i = 1; i < row.size(); i++)
			str += "," + row[i];
		result.push_back(str);
		return;
	}

	assert(table2name.find(table[depth]) != table2name.end());
	column_name = table2name[table[depth]];

	fin = fopen(((string) "data/" + table[depth]).c_str(), "r");
	assert(fin != NULL);

	while (fgets(buf, 65536, fin) != NULL) {
		int len = strlen(buf);
		if (len > 0 && buf[len - 1] == '\n') {
			buf[len - 1] = '\0';
			len--;
		}
		if (len == 0)
			continue;

		split_csv(buf, token);
		assert(token.size() == column_name.size());

		for (i = 0; i < column_name.size(); i++)
			if (m.find(column_name[i]) != m.end())
				row[m.find(column_name[i]) -> second] = token[i];

		done(table, m, depth + 1, row);
	}

	fclose(fin);
}

void create(const string& table_name, const vector<string>& column_name,
	const vector<string>& column_type, const vector<string>& primary_key)
{
	table2name[table_name] = column_name;
    int i;
    for(i = 0; i < column_name.size(); i ++) {
        col2table[column_name[i]] = table_name;
        col2no[column_name[i]] = i;
    }
	table2type[table_name] = column_type;
	table2pkey[table_name] = primary_key;
    //create the primary data base
	string str("data/");
	table2data[table_name] = new MyDb(str, table_name, false);
	table2data[table_name]->open(0);
    tableName.push_back(table_name);
}

void train(const vector<string>& query, const vector<double>& weight)
{
	/* I am too clever; I don't need it. */
    int i, j;
    vector<string> token;
    for(i = 0; i < query.size(); i ++) {
        token.clear();
        tokenize(query[i].c_str(), token);
        for(j = 0; j < token.size(); j ++) {
            if(token[j] != "WHERE")
                continue;
            else break;
        }
        for(j ++; j < token.size(); j ++) {
            if(token[j] == "AND" || token[j] == ";") {
                indexSet.insert(token[j-3]);
                indexSet.insert(token[j-1]);
            }
        }
    }
	string path("data/");
    set<string>::iterator iter = indexSet.begin();
    for(; iter != indexSet.end(); iter ++) {
        string& table_Name = col2table[*iter];
        int num = col2no[*iter];
        MyDb* currentDb = table2data[table_Name];
        if(currentDb == NULL)
            continue;
        string postfix = table_Name + string(".db");
        bool isInt;
        if(table2type[table_Name][num] == "INTEGER")
            isInt = true;
        else
            isInt = false;
        MyDb* index = new MyDb(path, *iter+postfix, true, isInt);
        index->set_flags(DB_DUPSORT);
        index->setColumnNo(col2no[*iter]);
        index->set_bt_compare(compare_dbt);
        index->set_bt_prefix(compare_prefix);
        index->open(0);
        currentDb->associate(*iter, NULL, (Db*)index, get_data, 0);
    }
}

void load(const string& table_name, const vector<string>& row)
{
	FILE *fout;
	int i, j;

	fout = fopen(((string) "data/tmp").c_str(), "a");
	assert(fout != NULL);

    MyDb *currentDb = table2data[table_name];
	unsigned int c = 0;
	Dbt key(&c, sizeof(int));
	Record record("");
	Dbt data(record.getBuffer(), record.getBufferSize());
    currentDb->put(NULL, &key, &data, DB_NOOVERWRITE);
	for (i = 0; i < row.size(); i++) {
        //**********************test********************//
		fprintf(fout, "%s\n", row[i].c_str());
        //**********************test********************//
        vector<string>& column_name = table2name[table_name];
        vector<string>& column_type = table2type[table_name];
        vector<string>& primary_key = table2pkey[table_name];
        //**********************load data***************//
		unsigned int c = currentDb->getCount();
		Dbt key(&c, sizeof(unsigned int));
		Record record(row[i].c_str());
		Dbt data(record.getBuffer(), record.getBufferSize());
        //cout << row[i].c_str() << endl;
        currentDb->put(NULL, &key, &data, DB_NOOVERWRITE);
        //cout << row[i].c_str() << endl;
        //**********************load data***************//
    }

	fclose(fout);
}

void preprocess()
{
	/* I am too clever; I don't need it. */
    // TODO
}

void insert(const vector<string>& table, const vector<string>& row)
{
    for(int i = 0; i < table.size(); i++) {
        MyDb *currentDb = table2data[table[i]];
        for(int j = 0; j < row.size(); j++) {
            unsigned int s = currentDb->getCount();
           	Dbt key(&s, sizeof(s));
            Record record(row[j].c_str());
		    Dbt data(record.getBuffer(), record.getBufferSize());
            currentDb->put(NULL, &key, &data, DB_NOOVERWRITE);
        }
    }
}

void select(const vector<string>& table, const map<string,int>& m, int depth, vector<string>& row)
{
   	vector<string> column_name;

    column_name = table2name[table[depth]];
    MyDb *currentDb = table2data[table[depth]];
    Dbc* cursorp;
    currentDb->cursor(NULL, &cursorp, 0);
    int column_num[50];
    int position[50];
    int k = 0;
    for (int j = 0; j < column_name.size(); j++) {
        if (m.find(column_name[j]) != m.end()) {
            column_num[k] = col2no[column_name[j]];
            position[k] = m.find(column_name[j])->second;
            k++;
        }
    }
    if (k > 0) {
        Dbt key,data;
        cursorp->get(&key, &data, DB_NEXT);
        int ret;
        while((ret = cursorp->get(&key, &data, DB_NEXT)) == 0) {
            Record record((Record::RecordBuffer*)data.get_data());
            for(int i = 0; i < k; i++)  {
                string str(record.getData(column_num[i]));
                row[position[i]] = str;
            }
            if(depth == table.size()-1) {
                string str = row[0];
                for (int i = 1; i < row.size(); i++)
                    str += "," + row[i];
				//cout << "result add" << str << endl;
                result.push_back(str);
            }
            else
                select(table,m,depth+1,row);
        }
    }
    return;
}

void selectWhere(const vector<string>& table, const map<string,int>& m, int depth, vector<string>& row, const vector<MyCondition>& condition, vector<string>& join_row, const map<string,vector<int> >& join_map, const map<string,int>& condition_map)
{
	//cout << "select where depth=" << depth << endl;
    string str;
    if(depth == table.size()){
        //cout << "depth=size" << endl;
        //cout << row[0] <<' ' << row[1] << ' ' << row[2] << endl;
        bool flag = true;
        for(int j = 0; j < join_row.size(); j = j+2) 
            if(join_row[j] != join_row[j+1]) {
                flag = false;
                //cout << "join failed" << join_row[j] << join_row[j+1] << endl;
                break;
            }
        if(flag){
		    str = row[0];
		    for (int i = 1; i < row.size(); i++) {
                str += "," + row[i];
		    }
            result.push_back(str);
            //cout << "result add " << str << endl;
            
        }
        return;
    }
    vector<string> column_name;
    column_name = table2name[table[depth]];
    MyDb *currentDb = table2data[table[depth]];
    Dbc* cursorp;
    currentDb->cursor(NULL, &cursorp, 0);
    int column_num[50], join_column_num[50], condition_column_num[50];
    int position[50], join_position[50], condition_position[50];
    int k = 0, l = 0, x = 0;
    for (int j = 0; j < column_name.size(); j++) {
        if (m.find(column_name[j]) != m.end()) {
            column_num[k] = col2no[column_name[j]];
            position[k] = m.find(column_name[j])->second;
			//cout << "column_name " << column_name[j] << endl;
			//cout << "column_num " << column_num[k] << endl; 
            k++;
        }
        if (join_map.find(column_name[j]) != join_map.end()) {
			for(int i = 0; i < join_map.find(column_name[j])->second.size(); i++) {
            	join_column_num[l] = col2no[column_name[j]];
            	join_position[l] = join_map.find(column_name[j])->second[i];
            	l++;
			}
        }
        if (condition_map.find(column_name[j]) != condition_map.end()) {
            condition_column_num[x] = col2no[column_name[j]];
            condition_position[x] = condition_map.find(column_name[j])->second;
            x++;
        }
    }
	//cout << table[depth] << endl;
	//cout << x << l << k << endl;
    if (x > 0 && (k > 0 || l > 0)) {
        //根据索引查找
        MyCondition my_condition = condition[condition_position[0]];
        MyDb *index = currentDb->get_second_database(my_condition.getOp1());
        char* search_name = my_condition.getOp2();
        Dbt key;
        int search;
        if(my_condition.isInt()) {
            search = atoi(search_name);
            key.set_data(&search);
            key.set_size(sizeof(int));
        } else {
            key.set_data(search_name);
            key.set_size(strlen(search_name)+1);
        }
        Dbt pkey,data;
        Dbc *cursorp;
        index->cursor(NULL, &cursorp, 0);
        u_int32_t Db_flag;
        int ret;
        bool equ = false;
        if(my_condition.getOptr() == '=') {
            Db_flag = DB_SET;
            ret = cursorp->pget(&key, &pkey, &data, Db_flag);
            Db_flag = DB_NEXT;
            equ = true;
        }
        else if(my_condition.getOptr() == '<') {
            cursorp->pget(&key, &pkey, &data, DB_SET_RANGE);
            Db_flag = DB_PREV;
            ret = cursorp->pget(&key,&pkey, &data, Db_flag);
        }
        else {
            search = atoi(search_name)+1;
            key.set_data(&search);
            ret = cursorp->pget(&key, &pkey, &data, DB_SET_RANGE);
            Db_flag = DB_NEXT;
        }
        if(ret == 0) {
            do{
                Record record((Record::RecordBuffer*)data.get_data());
                unsigned int *pkeyvalue = (unsigned int*)pkey.get_data();
                //cout << *pkeyvalue << endl;
                if(*pkeyvalue == 0) continue;
                bool flag = true;
                int kk = 0;
                if(!equ) kk = 1;
                for(int i = kk; i < x; i++) {
                    // 逐个判断条件
                    MyCondition conditions = condition[condition_position[i]];
                    //cout <<"record:" << (char*) record.getData(condition_column_num[i]) << endl;
                    if(!conditions.judge((char*)record.getData(condition_column_num[i]))) {
						//cout << (char*)record.getData(condition_column_num[i]) << endl;
                        //cout << "judge fail" << conditions.getOp1() << " " << conditions.getOptr() << " " << conditions.getOp2() << endl;
                        flag = false;
                        break;
                    }
                }
                if(flag) { 
                    for(int i = 0; i < k; i++)  {
                        string str(record.getData(column_num[i]));
                        row[position[i]] = str;
                        //cout << position[i] << ' ' << column_num[i] << " " << str << endl;
                    }
                    for(int i = 0; i < l; i++) {
                        join_row[join_position[i]] = record.getData(join_column_num[i]);
                    }
                    selectWhere(table, m, depth+1, row, condition, join_row, join_map, condition_map);
                }
            }while(cursorp->pget(&key,&pkey,&data,Db_flag) == 0); 
        }
    }
    else if (k > 0 || l > 0) {
        Dbt key,data;
        int ret;
        while((ret = cursorp->get(&key, &data, DB_NEXT)) == 0) {
            unsigned int *keyvalue = (unsigned int*)key.get_data();
            //cout << *pkeyvalue << endl;
            if(*keyvalue == 0) continue;
            Record record((Record::RecordBuffer*)data.get_data());
            for(int i = 0; i < k; i++)  {
                row[position[i]] = record.getData(column_num[i]);
            }
            for(int i = 0; i < l; i++) {
                join_row[join_position[i]] = record.getData(join_column_num[i]);
            }
            selectWhere(table, m, depth+1, row, condition, join_row, join_map, condition_map);
        }
    }
    else selectWhere(table, m, depth+1, row, condition, join_row, join_map, condition_map);
    return;
}

void execute(const string& sql)
{
	//cout << "execute" << endl;
	vector<string> token, output, table, row, join_row, join_col;
    vector<MyCondition> condition;
	map<string, int> m, condition_map;
	map<string, vector<int> > join_map;
	int i;

	result.clear();

	if (strstr(sql.c_str(), "INSERT") != NULL) {
        tokenize(sql.c_str(), token);
        table.clear();
        for(i = 0; i < token.size(); i++) {
            if(token[i] == "INSERT" || token[i] == "," || token[i] == "INTO")
                continue;
            if(token[i] == "VALUES")
                break;
            table.push_back(token[i]);
        }
        for(i++; i < token.size(); i++) {
            if(token[i] == "," || token[i] == ";")
                continue;
            row.push_back(token[i]);
        }
        insert(table, row);
        return;
    }

    output.clear();
    table.clear();
    tokenize(sql.c_str(), token);
    for (i = 0; i < token.size(); i++) {
        if (token[i] == "SELECT" || token[i] == ",")
            continue;
        if (token[i] == "FROM")
            break;
        output.push_back(token[i]);
    }
    string str = ""; 
    for (i++; i < token.size(); i++) {
        if (token[i] == "," || token[i] == ";")
            continue;
        if (token[i] == "WHERE")
            break;
        table.push_back(token[i]);
    }
    int k = 0;
    join_map.clear();
    join_col.clear();
    condition_map.clear();
    for (i++; i < token.size(); i++) {
		//cout << i << token[i] << endl;
        if (token[i] == "AND" || token[i] == ";") {
            //cout << "str=" << str.c_str() << endl;
            MyCondition myCondition((char*)str.c_str());
            str = "";
            if(myCondition.isCol()) {

                string str1(myCondition.getOp1());
                string str2(myCondition.getOp2());
                join_col.push_back(str1);
                join_col.push_back(str2);
				if(join_map.find(str1) == join_map.end()) {
					vector<int> vec;
					vec.push_back(k);
					join_map[str1] = vec;
				}
				else join_map[str1].push_back(k);
				if(join_map.find(str2) == join_map.end()) {
					vector<int> vec;
					vec.push_back(k+1);
					join_map[str2] = vec;
				}
				else join_map[str2].push_back(k+1);
                //join_map[str1] = k;
                //join_map[str2] = k+1;
                k = k + 2;
            }
            else {
                string str(myCondition.getOp1());
                condition_map[str] = condition.size();
                condition.push_back(myCondition);
            }
        }
		else
        	str = str + token[i] + " ";
    }
	//cout << i < endl;
    m.clear();
    for (i = 0; i < output.size(); i++)
        m[output[i]] = i;
    row.clear();
    row.resize(output.size(), "");
	
    join_row.clear();
    join_row.resize(join_col.size(), "");
    //cout << condition.size() << " " << join_col.size() << endl;
    /*for(int i = 0; i < condition.size(); i++) {
        cout << condition[i].getOp1() << " " << condition[i].getOptr() << " " << condition[i].getOp2() << endl;
    }*/
    //cout << "table size = " << table.size() << endl;
    //cout << "output size = " << output.size() << endl;
    if(condition.size() == 0 && join_col.size() == 0)
        select(table, m, 0, row);
    else selectWhere(table, m, 0, row, condition, join_row, join_map, condition_map);
    //cout << result.size() << endl;
    return;
}

int next(char *row)
{
	if (result.size() == 0)
		return (0);
	strcpy(row, result.back().c_str());
	result.pop_back();

	/*
	 * This is for debug only. You should avoid unnecessary output
	 * when submitting, which will hurt the performance.
	 */
	printf("%s\n", row);

	return (1);
}

void close()
{
	/* I have nothing to do. */
}

//*******************************my own functions***********************************//
int get_data(Db *t, const Dbt *pkey, const Dbt *pdata, Dbt *skey)
{
	MyDb* sdbp = (MyDb*)t;
    //make sure the pdata is the data of the column
    int columnNo = sdbp->getColumnNo();
    Record data((Record::RecordBuffer*)pdata->get_data());
	if (sdbp->isInt()) {
		volatile unsigned int d = data.getDataInteger(columnNo);
		skey->set_data((void*)&d);
		skey->set_size(sizeof(d));
	} else {
		const char* d = data.getDataString(columnNo);
        if(d == NULL)
            d = string("").c_str();
		skey->set_data(const_cast<char*>(d));
		skey->set_size(strlen(d)+1);
	}
    return (0);
}
