#include<iostream>
#include<stdio.h>
#include<vector>
#include<deque>
#include<set>
#include "StorageManager.h"
 
using namespace std;
 
enum TYPE{
		PLUS,
		MINUS,
		MULTIPLY,
		DIVIDE,
		EMPTY	};

 
//Attribute consists of table-name and column-name//
class Attribute {
private:
	string TblName;
	string ColName;
public:
	Attribute(string TblName, string ColName)
	{
		this->TblName=TblName;
		this->ColName=ColName;
	}
	string GetTblName()
	{
		return this->TblName;
	}
	string GetColName()
	{
		return this->ColName;
	}
	string GetType()
	{
		return "ATTRIBUTE";
	}

};

//Projection is a vector of attributes to be printed//

class Projection{
private:
	vector<Attribute*> _attributes;
public:
	Projection()
	{

	}
	void AddValue(Attribute* attr)
	{
		int i;
		for(i=0;i<this->GetSize();i++)
		{
			if(this->GetValue(i)->GetColName().compare(attr->GetColName())==0 && this->GetValue(i)->GetTblName().compare(attr->GetTblName())==0)
				break;
		}
		if(i>=this->GetSize())
		this->_attributes.push_back(attr);
	}

	int GetSize()
	{
		return this->_attributes.size();
	}
	Attribute* GetValue(int index)
	{
		if(index<_attributes.size() && index>=0)
			return _attributes[index];
	}
};

class ArithmeticOP {
public:

		void SetType(TYPE type)
		{
			this->oftype=type;
		}
		string GetType()
		{
			return "OPERATOR";
		}
		TYPE GetOPType()
		{
			return this->oftype;
		}
private:
	TYPE oftype;
};

class intVal {
private:
	int val;
public:
	void SetVal(int val)
	{
		this->val=val;
	}
	int GetVal()
	{
		return this->val;
	}
	string GetType()
	{
		return "INTEGER";
	}
};

class litVal{
private:
	string val;
public:
	void SetVal(string val)
	{
		this->val=val;
	}
	string GetVal()
	{
		return this->val;
	}
	string GetType()
	{
		return "LITERAL";
	}

};

//A factor can be an integer, char or attribute//
class Factor{
public:
	enum TYPE{
		INTEGER,
		ATTRIBUTE,
		LITERAL,
		OPERATOR};

		//Call when factor is an integer//
		Factor(TYPE type, intVal* val)
		{
			this->of_type=type;
			this->val=val;
		}
		
		//Call when factor is an operator. Ex. '+','*'//
		Factor(TYPE type, ArithmeticOP* op)
		{
			this->of_type=Factor::OPERATOR;
			this->op=op;
		}
		
		//Call when factor is an attribute//
		Factor(TYPE type, Attribute* att)
		{
			of_type=type;
			this->att=att;
		}

		Factor(TYPE type, litVal* lit)
		{
			of_type=type;
			this->litval=lit;
		}

		intVal* GetIntegerVal()
		{
			return val;
		}

		Attribute* GetAttribute()
		{
			return att;
		}

		ArithmeticOP* GetOperator()
		{
			return op;
		}

		litVal* GetLiteral()
		{
			return litval;
		}
		TYPE GetType()
		{
			return of_type;
		}

private:
	TYPE of_type;
	Attribute* att;
	ArithmeticOP* op;
	intVal* val;
	litVal* litval;
};


//Expression consists of factors// 
class Expression{
public:
	string ExpressionString;
	Expression()
	{
		this->icnt=0;
	}
		 void PushFactors(Factor* factor)
		 {
			 _expression.push_back(factor);
			 return;
		 }
	
		 Factor* GetFactor(int index)
		 {
			 return _expression[index];
		 }

		 int GetSize()
		 {
			 return _expression.size();
		 }
		 void GetExpressionString()
		 {
			 ///*string modified="((((";                                                                        
			 //for(int i=0;i!=ExpressionString.size();i++)
			 //{	
		 	//	switch(ExpressionString[i]){
				//case '(': modified.append("(((("); continue;
    //      		case ')': modified.append("))))"); continue;
    //      		case '*': modified.append("))*(("); continue;
    //      		case '/': modified.append("))/(("); continue;
    //      		case '+':
    //        	if (i == 1 || strchr("(^*/+-", ExpressionString[i-1]))
    //          	modified.append("+");
    //        	else
    //          	modified.append(")))+(((");
    //        	continue;
    //      		case '-':
    //        	if (i == 1 || strchr("(^*/+-", ExpressionString[i-1]))
    //          	modified.append("-");
    //        	else
    //          	modified.append(")))-(((");
    //        	continue;
    //  			}
				//modified.push_back(ExpressionString[i]);
    //
 			// }
  		//		modified.append("))))");
				//ExpressionString.assign(modified);
			 
			 return;

		 }

		 int GetExpressionVal(string Expression, Tuple& t, SchemaManager sm, string relationName)
		 {
			int currentpos=0;
			deque<int> _val;
			deque<ArithmeticOP*> _operator;
			for(int i=0;i<Expression.size();i++)
			{
			switch(Expression[i]){
			case '(':if(Expression[i+1]=='('){currentpos=Expression.find_last_of(')');
			 currentpos=Expression.rfind(')',currentpos-1);
			 _val.push_back(GetExpressionVal(Expression.substr(i+1,currentpos-i-1),t,sm,relationName)); i=currentpos;}
			 break;
			case ')':
				break;
			default:if(_expression[icnt]->GetType()==Factor::OPERATOR)
					{
						_operator.push_back(_expression[icnt]->GetOperator());
						icnt++;
					}
					else if(_expression[icnt]->GetType()==Factor::ATTRIBUTE)
					{
						int pos=sm.getSchema(relationName)->getFieldPos(_expression[icnt]->GetAttribute()->GetColName());
						_val.push_back(t.getInt(pos));
						icnt++;
					}
					else if(_expression[icnt]->GetType()==Factor::INTEGER)
					{
						_val.push_back(_expression[icnt]->GetIntegerVal()->GetVal());
						icnt++;
					}
					break;
			}
		
		}

		while(_operator.size()!=0)
		{
			int temp2=_val.back();
			_val.pop_back();
			int temp1=_val.back();
			_val.pop_back();
			switch(_operator.back()->GetOPType())
			{
			case TYPE::PLUS:_val.push_back(temp1 + temp2); break;
			case TYPE::MINUS:_val.push_back(temp1-temp2);break;
			case TYPE::DIVIDE:_val.push_back(temp1/temp2);break;
			case TYPE::MULTIPLY:_val.push_back(temp1*temp2);break;
			default:break;
			}
			_operator.pop_back();
		}

		this->icnt=0;
		return _val[0];
	}

	int GetJoinExpressionVal(string Expression, Tuple tA, string relA, Tuple tB, string relB, SchemaManager sm)
	{
			int currentpos=0;
			deque<int> _val;
			deque<ArithmeticOP*> _operator;
			for(int i=0;i<Expression.size();i++)
			{
			switch(Expression[i]){
			case '(':if(Expression[i+1]=='('){currentpos=Expression.find_last_of(')');
			 currentpos=Expression.rfind(')',currentpos-1);
			 _val.push_back(GetJoinExpressionVal(Expression.substr(i+1,currentpos-i-1),tA,relA,tB,relB,sm)); i=currentpos;}
			 break;
			case ')':
				break;
			default:if(_expression[icnt]->GetType()==Factor::OPERATOR)
					{
						_operator.push_back(_expression[icnt]->GetOperator());
						icnt++;
					}
					else if(_expression[icnt]->GetType()==Factor::ATTRIBUTE)
					{
						int pos;
						string _seltabA=_expression[icnt]->GetAttribute()->GetTblName().append("_seltemp");				 
						string _projA=_expression[icnt]->GetAttribute()->GetTblName().append("_proj");
				 
						if(relA.substr(0,relA.find_first_of('_')).compare(_expression[icnt]->GetAttribute()->GetTblName())==0)
						{
						pos=sm.getSchema(relA)->getFieldPos(_expression[icnt]->GetAttribute()->GetColName());
						_val.push_back(tA.getInt(pos));
						}
						else if(relB.substr(0,relB.find_first_of('_')).compare(_expression[icnt]->GetAttribute()->GetTblName())==0)
						{
						pos=sm.getSchema(relB)->getFieldPos(_expression[icnt]->GetAttribute()->GetColName());
						_val.push_back(tB.getInt(pos));
						}
						icnt++;
					}
					else if(_expression[icnt]->GetType()==Factor::INTEGER)
					{
						_val.push_back(_expression[icnt]->GetIntegerVal()->GetVal());
						icnt++;
					}
					break;
			}
		
		}

		while(_operator.size()!=0)
		{
			int temp2=_val.back();
			_val.pop_back();
			int temp1=_val.back();
			_val.pop_back();
			switch(_operator.back()->GetOPType())
			{
			case TYPE::PLUS:_val.push_back(temp1 + temp2); break;
			case TYPE::MINUS:_val.push_back(temp1-temp2);break;
			case TYPE::DIVIDE:_val.push_back(temp1/temp2);break;
			case TYPE::MULTIPLY:_val.push_back(temp1*temp2);break;
			default:break;
			}
			_operator.pop_back();
		}

		this->icnt=0;
		return _val[0];

	}

	bool checkifliteral(string relation, SchemaManager sm)
	{
		for(int i=0;i<_expression.size();i++)
		{
			if(_expression[i]->GetType()==Factor::LITERAL)
			{
				return true;
			}
			else if(_expression[i]->GetType()==Factor::ATTRIBUTE)
			{
				if(sm.getSchema(relation)->getFieldType(_expression[i]->GetAttribute()->GetColName()).compare("STR20")==0)
				{
					return true;
				}
			}
		}
		return false;
	}

	string GetLiteralVal(string Expression, Tuple& t, SchemaManager sm, string relationName)
	{
		if(_expression[0]->GetType()==Factor::ATTRIBUTE)
		{
		int pos=sm.getSchema(relationName)->getFieldPos(_expression[0]->GetAttribute()->GetColName());
		{
			return t.getString(pos);
		}
		}
		if(_expression[0]->GetType()==Factor::LITERAL)
		{
			string temp=_expression[0]->GetLiteral()->GetVal();
			string temp1;
			for(int i=0;i<temp.size();i++)
			{
				if(i!=0 && i!=temp.size()-1)
					temp1.push_back(temp[i]);
			}
			return temp1;
		}

	}
		 
private:
	deque<Factor*> _expression; 	
	 int icnt;
};


//A comparison operator class //
class CompOp{
public:
	enum TYPE{
		EQUAL,
		LESS_THAN,
		GREATER_THAN,
		EQUAL_OR_LESS_THAN,
		EQUAL_OR_GREATER_THAN
	};

		CompOp()
		{
		}

		CompOp(TYPE val)
		{
			of_CompOp=val;
		}

		void SetType(TYPE val)
		{
			of_CompOp=val;
		}
		
		TYPE GetCompOp()
		{
			return of_CompOp;
		}

private:
	TYPE of_CompOp;

};

//A Comparison Predicate class consists of left expression compop right expression //
class ComparisonPredicate{
private:
	Expression* left;
	CompOp* middle;
	Expression* right;
public:
	vector<string>ofTable; //Name of relations this comparison predicate belongs to/
	ComparisonPredicate()
	{
		
	}

	ComparisonPredicate(Expression* left, CompOp* middle, Expression* right)
	{
		this->left=left;
		this->middle=middle;
		this->right=right;
	}

	void SetComparisonPredicate(Expression* left, CompOp* middle, Expression* right)
	{
		this->left=left;
		this->middle=middle;
		this->right=right;
	}
	
	Expression* GetleftExpression()
	{
		return this->left;
	}

	CompOp* GetMiddleCompOp()
	{
		return this->middle;
	}

	Expression* GetRightExpression()
	{
		return this->right;
	}

	//checks if the comparison predicate is from just one relationand sets ofTable value//
	bool ifFromOneTable()
	{
 
		int count=0;
		for(int i=0;i<left->GetSize();i++)
		{
			if(left->GetFactor(i)->GetType()==Factor::ATTRIBUTE)
			{	
				 string temp=left->GetFactor(i)->GetAttribute()->GetTblName();
				 int i=0;
				 for(i=0;i<ofTable.size();i++)
				 {
					 if(temp.compare(ofTable[i])==0)
						 break;
				 }
				 if(i>=ofTable.size())
					ofTable.push_back(temp);
				
				
			}
		}
		for(int i=0;i<right->GetSize();i++)
		{
			if(right->GetFactor(i)->GetType()==Factor::ATTRIBUTE)
			{
				string temp=right->GetFactor(i)->GetAttribute()->GetTblName();
				 int i=0;
				 for(i=0;i<ofTable.size();i++)
				 {
					 if(temp.compare(ofTable[i])==0)
						 break;
				 }
				 if(i>=ofTable.size())
					ofTable.push_back(temp);	
			}
		}

		if(this->ofTable.size()<=1)
			return true;
		else
		{
			return false;
		}

	}
	bool ComputeExpression(Tuple& t, SchemaManager sm, string relationName)
	{
		if(this->left->checkifliteral(relationName,sm)==true || this->right->checkifliteral(relationName,sm)==true)
		{
			if(this->left->GetLiteralVal(this->left->ExpressionString,t,sm,relationName).compare(this->right->GetLiteralVal(this->right->ExpressionString,t,sm,relationName))==0)
				return true;
			else
				return false;
		}

		bool val=false;
		switch(middle->GetCompOp())
		{
		case CompOp::EQUAL:if(left->GetExpressionVal(left->ExpressionString, t, sm, relationName)==right->GetExpressionVal(right->ExpressionString, t, sm, relationName))
		val=true;break;
		case CompOp::EQUAL_OR_GREATER_THAN:if(left->GetExpressionVal(left->ExpressionString, t, sm, relationName)>=right->GetExpressionVal(right->ExpressionString, t, sm, relationName))
		val=true;break;
		case CompOp::EQUAL_OR_LESS_THAN:if(left->GetExpressionVal(left->ExpressionString, t, sm, relationName)<=right->GetExpressionVal(right->ExpressionString, t, sm, relationName))
		val=true;break;
		case CompOp::GREATER_THAN:if(left->GetExpressionVal(left->ExpressionString, t, sm, relationName)>right->GetExpressionVal(right->ExpressionString, t, sm, relationName))
		val=true;break;
		case CompOp::LESS_THAN:if(left->GetExpressionVal(left->ExpressionString, t, sm, relationName)<right->GetExpressionVal(right->ExpressionString, t, sm, relationName))
		val=true;break;
		default:break;
		}

		return val;
	}

	bool ComputeJoin(Tuple tA, string relA, Tuple tB, string relB, SchemaManager sm)
	{
		if(relA.substr(0,relA.find_first_of('_')).compare(this->left->GetFactor(0)->GetAttribute()->GetTblName())==0)
		{
			if(this->left->checkifliteral(relA,sm)==true)
			{
				if(this->left->GetLiteralVal(this->left->ExpressionString,tA,sm,relA).compare(this->right->GetLiteralVal(this->right->ExpressionString,tB,sm,relB))==0)
				return true;
			else
				return false;
			}
		}
		if(relB.substr(0,relB.find_first_of('_')).compare(this->left->GetFactor(0)->GetAttribute()->GetTblName())==0)
		{
			if(this->left->checkifliteral(relB,sm)==true)
			{
				if(this->left->GetLiteralVal(this->left->ExpressionString,tB,sm,relB).compare(this->right->GetLiteralVal(this->right->ExpressionString,tA,sm,relA))==0)
				return true;
			else
				return false;
			}
		}
		bool val=false;
		switch(middle->GetCompOp())
		{
		case CompOp::EQUAL:if(left->GetJoinExpressionVal(left->ExpressionString, tA,relA,tB,relB, sm)==right->GetJoinExpressionVal(left->ExpressionString, tA,relA,tB,relB, sm))
		val=true;break;
		case CompOp::EQUAL_OR_GREATER_THAN:if(left->GetJoinExpressionVal(left->ExpressionString, tA,relA,tB,relB, sm)>=right->GetJoinExpressionVal(left->ExpressionString, tA,relA,tB,relB, sm))
		val=true;break;
		case CompOp::EQUAL_OR_LESS_THAN:if(left->GetJoinExpressionVal(left->ExpressionString, tA,relA,tB,relB, sm)<=right->GetJoinExpressionVal(left->ExpressionString, tA,relA,tB,relB, sm))
		val=true;break;
		case CompOp::GREATER_THAN:if(left->GetJoinExpressionVal(left->ExpressionString, tA,relA,tB,relB, sm)>right->GetJoinExpressionVal(left->ExpressionString, tA,relA,tB,relB, sm))
		val=true;break;
		case CompOp::LESS_THAN:if(left->GetJoinExpressionVal(left->ExpressionString, tA,relA,tB,relB, sm)<right->GetJoinExpressionVal(left->ExpressionString, tA,relA,tB,relB, sm))
		val=true;break;
		default:break;
		}

		return val;
	}


};
//
//A search condition could be just one comparison predicate or a vector of it//
class SearchCondition{
private:
	vector<ComparisonPredicate*> _conditions;
public:
	SearchCondition()
	{

	}
	void AddComparisonPredicate(ComparisonPredicate* A)
	{
		_conditions.push_back(A);
	}
	ComparisonPredicate* GetComparisonPredicate(int index)
	{
		return _conditions[index];
	}
	int GetSize()
	{
		return _conditions.size();
	}

};

//node could be a leaf node (SEARCH CONDITION), or non-leaf UNION (OR), INTERSECTION(AND) 
//to be used using parsing of search condition
class node{
public:
	enum TYPE{
		SEARCH_CONDITION,
		INTERSECTION,
		UNION,
	};
	node()
	{
	
	}
	void AddChildren(node* X)
	{
		_children.push_back(X);
	}
	
	void AddComparisonPredicate(ComparisonPredicate* A)
	{
		_conditions.push_back(A);
	}
	int GetNoChildren()
	{
		return _children.size();
	}
	node* GetChild(int index)
	{
		return _children[index];
	}
	void SetType(TYPE type)
	{
		of_type=type;
	}

	TYPE GetType()
	{
		return of_type;
	}
	ComparisonPredicate* GetComparison()
	{
		return _conditions[0];
	}
private:
	vector<node*> _children;
	vector<ComparisonPredicate*> _conditions; //If SEARCH CONDITION, then we use this
	TYPE of_type;

};
//Table class to store projection,search condition, duplicate elimination operators associated with it//
class Table{
private:
	Projection* _projection;
	vector<node*> _condition;
	string _tblName;
	bool removeDuplicates;
public:
	Table()
	{
		_projection=NULL;
		removeDuplicates=false;
	}
	Table(string T)
	{
		_tblName=T;
		_projection=NULL;
		removeDuplicates=false;
	}
	Projection* GetProjection()
	{
		return this->_projection;
	}
	string GetTblName()
	{
		return this->_tblName;
	}
	void AddComparisonPredicate(node* cp)
	{
		_condition.push_back(cp);
	}
	void AddProjection(Projection* P)
	{
		this->_projection=P;
	}
	void SetDuplicateElimination()
	{
		this->removeDuplicates=true;
	}
	int getnoConditions()
	{
		return _condition.size();
	}
	node* GetComparisonPredicate(int index)
	{
		return _condition[index];
	}
	bool GetDuplicateElimination()
	{
		return this->removeDuplicates;
	}
};

//Producat class to have table pointers and join conditions//
class Product{
private:
	vector<node*>_conditions;
	vector<Table*>_tables;
	node* root;
public:
	Product()
	{

	}
	void AddJoinCondition(node* join)
	{
		_conditions.push_back(join);

	}	
	void AddTable(Table* A)
	{
		_tables.push_back(A);
	}
	vector<Table*> GetTables()
	{
		return _tables;
	}
	vector<node*> GetConditions()
	{
		return _conditions;
	}

	node* Getrootnode()
	{
		if(_conditions.size()<=0)
			return NULL;
		deque<node*> _queue;
		for(vector<node*>::iterator it=_conditions.begin();it!=_conditions.end();it++)
		{
			_queue.push_back(*it);
			if(_queue.size()==2)
			{
				node* _and=new node();
				_and->SetType(node::INTERSECTION);
				_and->AddChildren(_queue[0]);
				_and->AddChildren(_queue[1]);
				_queue.pop_back();
				_queue.pop_back();
				_queue.push_back(_and);
			}
		}
		return _queue[0];
	}

};


