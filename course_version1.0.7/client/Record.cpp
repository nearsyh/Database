#include "Record.h"
#include <cstdlib>
#include <cstring>
#include <string>
#include <sstream>

Record::RecordBuffer* Record::createBuffer(int buflen){
	RecordBuffer* ret = (RecordBuffer*)malloc(buflen + sizeof(RecordBuffer) - 1);
	ret->buflen = buflen;
	ret->column = 0;
	*ret->buffer = 0;

    return ret;
}
void Record::freeBuffer(Record::RecordBuffer* buf){
	free(buf);
}
/*Record::RecordBuffer* Record::cloneBuffer(Record::RecordBuffer* buf) {
	RecordBuffer* ret = (RecordBuffer*)malloc(buf->buflen + sizeof(RecordBuffer) - 1);
	ret->buflen = buf->buflen;
	ret->column = buf->column;
	memcpy(ret->buffer, buf->buffer, ret->buflen);
    
    return ret;
}*/

Record::Record()
	: buffer(0)
{
}
Record::Record(const char* recordString) {
	int strl = strlen(recordString);
	buffer = createBuffer(strl + 2);
	memcpy(buffer->buffer, recordString, strl);
	buffer->buffer[strl] = buffer->buffer[strl + 1] = 0;
	buffer->column = 0;
	char* p = buffer->buffer;

	int begin = 0, end = 0;
	while(*p != 0) {
		if (*p == ',') {
			FieldTag& tag = buffer->fieldTags[buffer->column++];
			tag.begin = begin;
			tag.end = end;
			tag.isInteger = !(buffer->buffer[begin] == '\'');
			if (tag.isInteger) {
				*p = 0;
				tag.num = atoi(&buffer->buffer[begin]);
				*p = ',';
			} else {
				++tag.begin;
				--tag.end;
				buffer->buffer[tag.end] = 0;
			}

			begin = end + 1;
		}
		++p;
		++end;
	}
    {
        FieldTag& tag = buffer->fieldTags[buffer->column++];
        tag.begin = begin;
        tag.end = end;
        tag.isInteger = !(buffer->buffer[begin] == '\'');
        if (tag.isInteger) {
            tag.num = atoi(&buffer->buffer[begin]);
        } else {
            ++tag.begin;
            --tag.end;
            buffer->buffer[tag.end] = 0;
        }
    }
}

Record::Record(RecordBuffer* buffer)
	: buffer(buffer)
{
}

const char* Record::getData(int i) const {
	if (buffer == 0 || i >= buffer->column) return "";
	
	static std::ostringstream buf;
	if (buffer->fieldTags[i].isInteger) {
        buf.str("");
        buf << getDataInteger(i);
		return buf.str().c_str();
	} else {
		buf.str("");
		buf << '\'' << getDataString(i) << '\'';
		return buf.str().c_str();
	}
}
//i refers the column
const char* Record::getDataString(int i) const {
	if (buffer == 0 || i >= buffer->column) return 0;
	
	if (buffer->fieldTags[i].isInteger) return 0;

	return &buffer->buffer[buffer->fieldTags[i].begin];
}

Record::IntegerType Record::getDataInteger(int i) const {
	if (buffer == 0 || i >= buffer->column) return 0;

	if (!buffer->fieldTags[i].isInteger) return 0;

	return buffer->fieldTags[i].num;
}

bool Record::isDataInteger(int i) const {
	if (buffer == 0 || i >= buffer->column) return false;
	return buffer->fieldTags[i].isInteger;
}

