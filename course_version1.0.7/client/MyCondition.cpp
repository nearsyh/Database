#include <string.h>
#include <stdlib.h>
#include "MyCondition.h"

MyCondition::MyCondition(char* cond) {
	_isInt = _isCol = _isStr = false;
	int length(0), optrIndex(0), flag(0); 
	cond[strlen(cond)-1] = '\0';
	while(cond[length] != '\0' && cond[length] != ' ') {
		length++;
	}
	optrIndex = length+1;
	while(cond[length] != '\0') {
		if (cond[length] == '\'') {
			flag = 1;
		}
		length++;
	}

	if (flag) {
		_isStr = true;
	}

	condition = new char[length+1];
	op1 = new char[optrIndex];
	op2 = new char[length-optrIndex-1];

	strcpy(condition, cond);
	strncpy(op1, condition, optrIndex-1);
	op1[optrIndex-1] = '\0';
	strcpy(op2, condition+optrIndex+2);
	optr = condition[optrIndex];

	if (!_isStr) {
		if (op2[0] < 48 || op2[0] > 57) _isCol = true;
		else _isInt = true;
	}
}
bool MyCondition::isInt(){
	return _isInt;
}
bool MyCondition::isCol(){
	return _isCol;
}
bool MyCondition::isStr(){
	return _isStr;
}
char* MyCondition::getOp1(){
	char* temp = new char[strlen(op1)+1];
	strcpy(temp, op1);
	return temp;
}
char* MyCondition::getOp2() {
	char* temp;
	if (_isStr) {
		temp = new char[strlen(op2)];
		strcpy(temp, op2+1);
		temp[strlen(op2)-2] = '\0';
	} else {
		temp = new char[strlen(op2)+1];
		strcpy(temp, op2);
	}
	return temp;
}
char MyCondition::getOptr() {
	return optr;
}
bool MyCondition::judge(char* ctemp) {
	if (_isInt) {
		int cri = strlen(op2);
		int givin = strlen(ctemp);
		int index(0);
		switch(optr){
		case '<': if (cri < givin) return false;
				  else if (cri > givin){
					  return true;
				  } else {
					  while(index <= cri-1) {
						  if (op2[index] < ctemp[index]) return false;
						  else if (op2[index] > ctemp[index]) return true;
						  else index++;
					  }
					  return false;
				  }
		case '>': if (cri < givin) return true;
				  else if (cri > givin){
					  return false;
				  } else {
					  while(index <= cri-1) {
						  if (op2[index] < ctemp[index]) return true;
						  else if (op2[index] > ctemp[index]) return false;
						  else index++;
					  }
					  return false;
				  }
		case '=': return (strcmp(op2, ctemp) == 0);
		default: return false;
		}
	} 
	if (_isStr) return (strcmp(op2, ctemp) == 0);
}
