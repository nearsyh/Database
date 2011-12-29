#ifndef RECORD_H
#define RECORD_H
#include <vector>

class Record
{
public:
	typedef unsigned int IntegerType;
public:
	struct FieldTag
	{
		int begin, end;
		bool isInteger;
		unsigned int num;
	};
	struct RecordBuffer
	{
		int column;
		int buflen;
		FieldTag fieldTags[51];
		char buffer[1];
	};
private:
	RecordBuffer* buffer;
	
	static RecordBuffer* createBuffer(int buflen);
	static void freeBuffer(RecordBuffer* buf);
	//static RecordBuffer* cloneBuffer(RecordBuffer*);

	void aboutToChangeBuffer();
private:
	Record(const Record&);//forbidden
	Record& operator=(const Record&);//forbidden
public:
	Record();
    //build a record from a string. In which fields are seperated by ',', and string is surrounded by '''.
	explicit Record(const char* recordString);
    //build a record from a buffer.
	Record(RecordBuffer* buffer);

	int getColumn() const { if (buffer) return buffer->column; return 0; }
    //always return a null-terminated string
    //if column exceeds size, then "" is returned.
	const char* getData(int column) const;
    //if the type of data is string, a null-terminated string is returned;
    //otherwise null is returned.
	const char* getDataString(int column) const;
    //if the type of data is integer, it will be returned;
    //otherwise 0 is returned.
	IntegerType getDataInteger(int column) const;
    //test whether the data is integer.
	bool isDataInteger(int i) const;

    //for db storage
    //get the buffer
	RecordBuffer* getBuffer() { return buffer; }
    //get the size of buffer
	int getBufferSize() { if (buffer) return buffer->buflen + sizeof(RecordBuffer) - 1; return 0; }
};

#endif
