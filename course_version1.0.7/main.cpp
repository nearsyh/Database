#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <cassert>
#include <sys/time.h>

#include "lib/hash.h"
#include "include/client.h"

#define PATH		"test/"
#define SCHEMA		(PATH "schema")
#define STATISTIC	(PATH "statistic")
#define QUERY		(PATH "query")

using namespace std;

int main()
{
	FILE *fin;
	char buf[65536], buf2[65536];
	int i, j, n, m, res;
	string table_name;
	vector<string> table_list, column_name, column_type;
	vector<string> primary_key, query, row;
	vector<double> weight;
	struct timeval start, end;

	/* Schema */

	fin = fopen(SCHEMA, "r");
	assert(fin != NULL);

	res = fscanf(fin, "%d", &n);
	assert(res == 1);
	for (i = 0; i < n; i++) {
		res = fscanf(fin, "%s", buf);
		assert(res == 1);
		table_name = buf;
		table_list.push_back(table_name);

		column_name.clear();
		column_type.clear();
		res = fscanf(fin, "%d", &m);
		assert(res == 1);
		for (j = 0; j < m; j++) {
			res = fscanf(fin, "%s %s", buf, buf2);
			assert(res == 2);
			column_name.push_back(buf);
			column_type.push_back(buf2);
		}

		primary_key.clear();
		res = fscanf(fin, "%d", &m);
		assert(res == 1);
		for (j = 0; j < m; j++) {
			res = fscanf(fin, "%s", buf);
			assert(res == 1);
			primary_key.push_back(buf);
		}

		create(table_name, column_name, column_type, primary_key);
	}

	fclose(fin);

	/* Statistic */

	fin = fopen(STATISTIC, "r");
	assert(fin != NULL);

	query.clear();
	weight.clear();
	res = fscanf(fin, "%d", &n);
	assert(res == 1);
	for (i = 0; i < n; i++) {
restart_1:
		char *str = fgets(buf, 65536, fin);
		assert(str == buf);
		int len = strlen(buf);
		if (len > 0 && buf[len - 1] == '\n') {
			buf[len - 1] = '\0';
			len--;
		}
		if (len == 0)
			goto restart_1;
		query.push_back(buf);

		double temp;
		res = fscanf(fin, "%lf", &temp);
		assert(res == 1);
		weight.push_back(temp);
	}

	train(query, weight);

	fclose(fin);

	/* Load initial data */

	for (i = 0; i < table_list.size(); i++) {
		sprintf(buf, PATH "%s.data", table_list[i].c_str());
		fin = fopen(buf, "r");
		assert(fin != NULL);

		row.clear();
		res = fscanf(fin, "%d", &n);
		assert(res == 1);
		for (j = 0; j < n; j++) {
			res = fscanf(fin, "%s", buf);
			assert(res == 1);
			row.push_back(buf);
			if (row.size() == 65536) {
				load(table_list[i], row);
				row.clear();
			}
		}
		if (row.size() > 0)
			load(table_list[i], row);

		fclose(fin);
	}


	/* Preprocessing */

	preprocess();

	/* Execute queries */
	res = gettimeofday(&start, NULL);
	assert(res == 0);

	fin = fopen(QUERY, "r");
	assert(fin != NULL);

	res = fscanf(fin, "%d", &n);
	assert(res == 1);
	for (i = 0; i < n; i++) {
restart_2:
		char *str = fgets(buf, 65536, fin);
		assert(str == buf);
		int len = strlen(buf);
		if (len > 0 && buf[len - 1] == '\n') {
			buf[len - 1] = '\0';
			len--;
		}
		if (len == 0)
			goto restart_2;

		execute(buf);

		if (strstr(buf, "INSERT") != NULL)
			continue;

		unsigned int checksum = 0;
		while (next(buf2))
			checksum += hash(buf2);

		printf("Checksum: %X\n--------\n", checksum);
	}

	fclose(fin);

	res = gettimeofday(&end, NULL);
	assert(res == 0);

	double interval = (double) (end.tv_sec - start.tv_sec) +
		(end.tv_usec - start.tv_usec) / 1000000.0;
	printf("Response time: %.3lf sec\n", interval);

	/* Close */

	close();

	return (0);
}


