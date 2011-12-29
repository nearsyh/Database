class MyCondition{
	char* condition;
	char* op1;
	char* op2;
	char optr;
	bool _isInt;
	bool _isStr;
	bool _isCol;
public:
	MyCondition(char* cond);
	bool isInt();
	bool isCol();
	bool isStr();
	char* getOp1();
	char* getOp2();
	char getOptr();
	bool judge(char* ctemp);
};
