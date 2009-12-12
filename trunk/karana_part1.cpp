#ifndef _StorageManager_h_
	#include "StorageManager.h"
	#define _StorageManager_h_ 
#endif
#include <iostream>
#include <string>
#include <exception>
#include <regex>
#include <map>
#include<conio.h>
#include<stdlib.h>
#include<string.h>
#include<sstream>
#include<vector>
#include "QueryManager.cpp"
#include "Markup.h"

using namespace std;
using namespace std::tr1;

#define MAXWORDS 1000
extern "C" char* words[MAXWORDS];
extern "C" int totalwords;

extern "C" int yyparse(void);
extern "C" int yylex(void);
extern "C" FILE *yyin;

//Global memory and disk objects
extern MainMemory mem;
extern SchemaManager schemaMgr;
extern map<string,node*> ConditionMap;
extern vector<node*>conditions;
extern Projection P;
extern vector<Table*>T;
extern Product Pr;
extern int run_query (string , int , int, int );

//function prototypes
int callLex (string );
int create_table(string , map<string,string>);
int delete_from_table(string, string);
int insert_into_table(string, map<string,string>);
void ParseTree();
node* ParseCondition(string expr);
void returnCondition(node* root);
vector<string> checksubtree(node* root);
void CreateDebugData();
vector<string> split_word(string ,string );
vector<string> split (string ,string );
string trim(string&);

/**
* This method converts the query to its logical plan
* @param string query: The query
*/
int query2logical(string query) 
{
	//cout << "In: The memory contains " << mem.getMemorySize() << " blocks" << endl << endl;
	
	cout << "The query is: " << query << endl;
	string firstword = "", thisquery = "";
	thisquery = query;
	size_t indexx = thisquery.find_first_of(' ');

	// Use regex 
	try
	{
		cmatch res;
		string insert_string, drop_table_string, create_table_string, delete_string, desc_table_string;
		
		// Initialize the regex patterns for different types of statements.
		insert_string = "^insert into ([[:alnum:]]*)";
		insert_string += "[[:space:]]?([(]([[:alnum:], ]*)[)])*";
		insert_string += "[[:space:]]?((values[[:space:]]?([(]([[:alnum:], '\"]*)[)])*";
		insert_string += ")|((select) .*))";

		regex insert_pattern (insert_string, std::tr1::regex_constants::icase);
		
		drop_table_string = "^drop table ([[:alnum:]]*)";
		regex drop_table_pattern (drop_table_string, std::tr1::regex_constants::icase);
		
		desc_table_string = "^desc ([[:alnum:]]*)";
		regex desc_table_pattern (desc_table_string, std::tr1::regex_constants::icase);
		
		create_table_string = "^create table ([[:alnum:]]*)";
		create_table_string += "[[:space:]]?[(](.*)[)]";
		regex create_table_pattern (create_table_string, std::tr1::regex_constants::icase);
		
		regex select_pattern ("^select .*",std::tr1::regex_constants::icase);

		delete_string = "^delete from ([[:alnum:]]*)( where (.*))*";
		regex delete_pattern (delete_string, std::tr1::regex_constants::icase);
	
		//Check which type of statement the query is.
		if (regex_match (thisquery.c_str(), res, insert_pattern ))
		{
			//THIS IS AN INSERT QUERY
			/*
			cout << endl;
			cout << "INSERT QUERY:" << endl;
			cout << "Table: " << res[1] << endl;
			cout << "Attributes: " << res[3] << endl;			
			cout << "Values: " << res[7] << endl;
			
			for (int i = 0; i< res.size(); i++)
			{
				cout << i << " :" << res[i] << endl;
			}
			
			*/
			bool has_attr = false;
			string attrs = res[3];
			if (attrs.length() > 0)
				has_attr = true;

			//If it has a select query
			if (res[9] == "select" || res[9] == "SELECT")
			{
				// we need to run the select query first
				run_query(res[5],0,0,1);

				//get the results into memory and call insert_table function
			}
			else
			{
				//we got the values type of insert statement.
				
				vector<string> values = split (res[7],",");
				if (has_attr)
				{ // we have attributes specified in the statement
					vector<string> attributes = split (res[3],",");

					//we have same number of columns and column values
					if (attributes.size() != values.size())
					{
						cout << "Syntax Error: You have given " << attributes.size() << " column names"
							<< " but " << values.size() << " values. " 
							<< "Please enter the correct values for the correct columns." << endl;
					}
					else
					{
						map<string,string> insert_values;
						pair<map<string, string>::iterator,bool> ret;
	 			
						for (int i = 0; i < attributes.size() ; i++)
						{
							ret = insert_values.insert(pair<string,string>(
								trim(attributes[i]), trim(values[i])));
							if (ret.second == false)
							{
								cout << "Syntax error: You have given the same column name twice." << endl;
							}
						}

						insert_into_table(res[1], insert_values);
					}
				}
				else 
				{
					cout << "Your insert query doesnot specify any column names for the table. Please do that and re-enter." << endl;
				}
			}
			
			cout << "Values inserted successfully." << endl;
			return 3;
		}
		else if (regex_match (thisquery.c_str(), res, desc_table_pattern ))
		{
			//THIS IS A DESC TABLE QUERY
			if (schemaMgr.getRelation(res[1]) == NULL)
			{
				cout << "Sorry. This table does not exist." << endl;
				return -1;
			}
			else
			{
				cout << endl;
				schemaMgr.getSchema(res[1])->printSchema();

			}
			return 3;
		}
		else if (regex_match (thisquery.c_str(), res, drop_table_pattern ))
		{
			// THIS IS A DROP TABLE QUERY
			/*
			cout << endl;
			cout << "DROP TABLE QUERY:" << endl;
			cout << "Table Name: " << res[1] << endl;
			*/

			//check if the table exists or not
			if (schemaMgr.getRelation(res[1]) == NULL)
			{
				cout << "Sorry. This table does not exist." << endl;
				return -1;
			}
			else
			{
				schemaMgr.deleteRelation(res[1]);

				//test if table still exists
				if (schemaMgr.getRelation(res[1]) != NULL)
				{
					cout << "Internal Error: Not able to delete this relation." << endl;
					return -1;
				}

			}

			cout << "Table " << res[1] << " deleted successfully." << endl;

			return 3;

		}
		else if (regex_match (thisquery.c_str(), res, create_table_pattern ))
		{
			//THIS IS A CREATE TABLE QUERY

			//check if the table already exists
			if (schemaMgr.getRelation(res[1]) == NULL)
			{
				/*cout << endl;
				cout << "CREATE TABLE QUERY:" << endl;
				cout << "Table Name: " << res[1] << endl;
				cout << "Columns: " << res[2] << endl;*/

				map<string, string> schemadata; //This stores the data for the schema
				vector<string> cols, columndata; // Work vectors
				
	 			pair<map<string, string>::iterator,bool> ret;
	 			cols = split(res[2], ","); // Tokenize on ',' to get separate column

				for ( int i = 0; i < cols.size(); i++)
				{
					cols[i] = trim(cols[i]);
					columndata = split (cols[i], " "); //Tokenize on ' ' to get name and type
					
					if (columndata.size() < 2) 
					{
						cout << "Syntax error:: Column should be specified as columnname datatype in "
								<< " CREATE TABLE statement." << endl;
						return -1;						
					}
					else
					{
						//set schemadata for that column
						if (columndata[1] == "int" || columndata[1] == "INT" )
						{
							ret = schemadata.insert(
									pair<string,string>(columndata[0],"INT"));
						}
						else if (columndata[1] == "str20" || columndata[1] == "STR20" )
						{
							ret = schemadata.insert(
									pair<string,string>(columndata[0],"STR20"));
						}
						else
						{
							cout << "Syntax error:: Datatypes for a column can be INT or STR20." << endl;
								return -1;
						}

						if (ret.second == false)
						{
								cout << "Syntax error:: You have given two columns of same name." << endl;
								return -1;
						}
					}
				}
			
				//call function to create table using the schema data
				create_table(res[1], schemadata);

				//test if table exists now
				if (schemaMgr.getRelation(res[1]) == NULL)
				{
					cout << "Internal Error: Not able to create this relation." << endl;
					return -1;
				}

			}
			else
			{
				cout << "Syntax error: A table with name " << res[1] << " already exists." << endl;
				return -1;
			}
		
			return 3;
 		}
		else if (regex_match (thisquery.c_str(), res, select_pattern ))
		{
			//THIS IS A SELECT QUERY
			/*
			cout << endl;
			cout << "SELECT QUERY:" << endl;
			*/
			return callLex( thisquery );

		}
		else if (regex_match (thisquery.c_str(), res, delete_pattern ))
		{
			//THIS IS A DELETE QUERY
			/*
			cout << endl;
			cout << "DELETE QUERY:" << endl;
			cout << "Table Name: " << res[1] << endl;
			cout << "Condition: " << res[3] << endl;
			*/

			delete_from_table(res[1], res[3]);
			
			return 3;
		}
		else
		{
			cout << "Please check your query and try again." << endl;
			return -1;
		}
		
	}
	catch (exception& e)
	{
		cout << "We have an internal error...\n" << e.what() << endl;
	}

}

/**
* This method calls the lexical parser for SFW queries.
*/

int callLex ( string selectquery )
{
		CreateDebugData();
char buf[1000];
int pos=0;
FILE *fp;
char* var;
FILE* myfile;

//printf("\nPlease enter the query:\n\n");
//gets(buf);
string str(selectquery);


regex open_par("[(]",std::tr1::regex_constants::icase);
string operbr_replacement = "((";
str = regex_replace(str, open_par, operbr_replacement);

regex close_par("[)]",std::tr1::regex_constants::icase);
string closebr_replacement = "))";
str = regex_replace(str, close_par, closebr_replacement);

regex where_par("WHERE",std::tr1::regex_constants::icase);
string where_replacement = "WHERE (";
str = regex_replace(str, where_par, where_replacement);

regex or_par("OR" , std::tr1::regex_constants::icase);
string or_replacement=") OR (";
str=regex_replace(str,or_par,or_replacement);

if(str.find("WHERE")!=string::npos)
str.append(")");


cout << str << endl;
fp=fopen("temp.txt","w");
fprintf(fp,"%s",str.c_str());
fclose(fp);

//
//int firstbr=str.find_first_of("(");
//str.assign(str.substr(firstbr,str.size()-firstbr));
//regex condition_par("[a-z.<>=]+",std::tr1::regex_constants::icase);
//string conditionbr_replacement = "xx";
//str = regex_replace(str, condition_par, conditionbr_replacement);
//
//cout<<"\nnew str is: "<<str;

//getch(); //first getch() to check new string
	myfile = fopen("temp.txt", "r");

	if (!myfile) {

		return -1;
	}

	yyin = myfile;
 
	do {
		yyparse();
	} while (!feof(yyin));
fclose(myfile);

fp=fopen("temp.xml","w");
fprintf(fp,"<?xml version=\"1.0\"?>\n");
printf("total:%d",totalwords);
//getch();
for(int i=0;i<totalwords;i++)
{
	printf("%s",words[i]);
fprintf(fp,"%s",words[i]);
}
fclose(fp);
ParseTree();
	return 0;
}

/**
* THis method creates a table schema using schemadata
*/
int create_table(string table_name, map<string,string> column_name_type)
{
	map<string,string>::iterator it;
	//Create the schema using column names from the map
	vector<string> fieldNames;
	vector<string> fieldTypes;

	for ( it=column_name_type.begin() ; it != column_name_type.end(); it++ )
	{
		//Add name and type to respective vectors
		fieldNames.push_back((*it).first);
		fieldTypes.push_back((*it).second);
	}
	
	//Create the schema
	Schema schema(fieldNames,fieldTypes);

	// Print the information about the schema
 	//schema.printSchema();

	// Create a relation through the schema manager
	Relation* relationPtr=schemaMgr.createRelation(table_name,schema);

	// Print the information provided by the schema manager
	/*cout << "After creating a relation, current schemas and relations: " << endl;
	schemaMgr.printRelationSchemaPairs();
	cout << "From the schema manager, the table " << table_name << " has schema:" << endl;
	schemaMgr.getSchema(table_name)->printSchema();
	cout << "From the schema manager, the table " << table_name << " has name:" <<
	schemaMgr.getRelation(table_name)->getRelationName() << endl << endl;*/

	cout << "Table " << table_name << " created successfully." << endl;
	return 0;
}

/**
* This method is used to delete tuples from a table.
*/
int delete_from_table(string table_name, string condition)
{
	Relation* thisrelation = schemaMgr.getRelation(table_name);
	if(condition.empty())
	{
		thisrelation->deleteBlock(0);
	}
	else
	{
		//call one_pass_selection 
		//delete the tuples from the resultant relation
	}
	//thisrelation->printRelation();
	return 0;
}

/**
* This method is used to insert tuples into a table.
*/
int insert_into_table(string table_name, map<string,string> values)
{
	map <string, string>::iterator it;
	Schema* thisschema = schemaMgr.getSchema(table_name);
	Relation* thisrelation = schemaMgr.getRelation(table_name);

	// Set up a block in the memory
	Tuple tuple (thisschema);
  	Block* block=mem.getBlock(0); //access to memory block 0
	block->clear();

	for (it=values.begin(); it != values.end(); it++)
	{
		string type = thisschema->getFieldType((*it).first);
		string val = (*it).second;
		if (type.empty())
		{
			cout << "Syntax error: No column of name " << (*it).first
				<< " present in table" << table_name << endl;
			return -1;
		}
		if (type == "INT" && val.find_first_not_of("0123456789") != string::npos)
		{
			cout << "Syntax error: " << val << " is not an integer as expected for"
				<< " column " << (*it).first << endl;
			return -1;
		}
		if (type == "STR20" && val.size() > 20 )
		{
			cout << "Syntax error: " << val << " is not a string of 20 characters"
				"as expected for column " << (*it).first << endl;
			return -1;
		}


		if (type == "INT")
		{
			int valu = atoi(val.c_str());
			int pos = thisschema->getFieldPos((*it).first);
			tuple.setField(pos,valu);

		}
		else
		{
			tuple.setField(thisschema->getFieldPos((*it).first),val);
		}
		
	}
	//tuple.printTuple();
		
	int lastBlock;
	//Find if the last block of relation is empty
	if (thisrelation->getNumOfBlocks() != 0)
	{
		lastBlock = thisrelation->getNumOfBlocks()-1;	
		thisrelation->readBlockToMemory(lastBlock,0);
	}
	else
	{
		lastBlock = 0;
	}

	if (block->isFull())
	{
		//since relation has a full last block we need to add a new block
		block->clear();
		lastBlock++;
	}
		
	block->appendTuple(tuple);

	thisrelation->writeBlockFromMemory(lastBlock, 0);

	//thisrelation->printRelation();

	return 0;
}


/**
* This method is used to tokenize a string wrt a given delimiter.
*/
vector<string> split (string splitstring, string delim)
{
	vector<string> splits;
	char * pch;
	char* context	= NULL;
	
	pch = strtok_s ((char *)splitstring.c_str(),delim.c_str(), &context);
	while (pch != NULL)
  	{
		splits.push_back((string)pch);
		pch = strtok_s (NULL, delim.c_str(), &context);
  	}
  return splits;
}


/**
* Finds all instances of words in a given string and tokenizes accordingly.
*/
vector <string > split_word (string splitstring, string splitword)
{
	vector <string> returnvals;
	size_t found;
	while ((found = splitstring.find(splitword)) != string::npos)
	{
		returnvals.push_back(trim (splitstring.substr(0,found)));
		splitstring = splitstring.substr(found + splitword.size(), splitstring.size());
	}

	returnvals.push_back (trim (splitstring));
	return returnvals;
}
/**
* This method is used to trim or remove leading and trailing space from a string.
*/
string trim(string& o) {
  string ret = o;
  const char* chars = "\n\t\v\f\r ";
  ret.erase(ret.find_last_not_of(chars)+1);
  ret.erase(0, ret.find_first_not_of(chars));
  return ret;
}


 void ParseTree()
{	
	string ConditionExpr="";
	int CPcount=0;
	bool ifDISTINCT=false;
	Attribute* ifOrderBy=NULL;
	CMarkup xml;
	std::wstring str=L"temp.xml";
	if(!xml.Load(str))
	{
	printf("\nIncorrect XML file format!");
	}
	xml.ResetPos();
	xml.FindElem(L"Query");
	xml.FindChildElem(L"SFW");
	if(xml.GetChildAttrib(L"DISTINCT")==L"TRUE")
		ifDISTINCT=true;
	wstring order;
	int dpos=0;
	if((order=xml.GetChildAttrib(L"ORDERBY"))!=L"NULL")
	{
		string sorder(order.begin(),order.end());
		dpos=sorder.find_first_of(L'.')+1;
		ifOrderBy=new Attribute(sorder.substr(0,dpos-1),sorder.substr(0));
	}
	xml.IntoElem();
	xml.FindChildElem(L"Attributes");
	xml.IntoElem();
	while(xml.FindChildElem(L"COLUMN_NAME"))
	{
		wstring temp=xml.GetChildData();
		int pos=temp.find_first_of(L'.')+1;
		wstring col=temp.substr(pos);
		string colName(col.begin(),col.end()); 
		wstring tbl=temp.substr(0,pos-1);
		string tblName(tbl.begin(),tbl.end());
		Attribute *one=new Attribute(tblName,colName);
		P.AddValue(one);
	}
 
 	xml.OutOfElem();
	xml.ResetChildPos();
	xml.FindChildElem(L"Relations");
	xml.IntoElem();
	while(xml.FindChildElem(L"TABLE_NAME"))
	{
		wstring temp=xml.GetChildData();
		string tblName(temp.begin(),temp.end());
		// check if table exists //
		if(schemaMgr.getRelation(tblName)==NULL)
		{
			printf("\nTable does not exist");
			exit(-1);
		}
		Table *one=new Table(tblName);
		T.push_back(one);
	}
	xml.OutOfElem();
	xml.ResetChildPos();
	if(xml.FindChildElem(L"Search_Condition"))
	{
	xml.IntoElem();
	while(xml.FindChildElem())
	{
		wstring ws_srch=xml.GetChildTagName();
		if(ws_srch==L"ComparisonPredicate")
		{
		node *SC=new node();
		ComparisonPredicate *CP=new ComparisonPredicate();
		Expression *left=new Expression();
		CompOp *middle=new CompOp();
		Expression *right=new Expression();
		int count=0;
		
		xml.IntoElem();
		while(xml.FindChildElem()) //left expression, comop or right expression
		{
			wstring ws_exp=xml.GetChildTagName();
			if(ws_exp==L"Expression")
			{
			
				xml.IntoElem();
				int openbrs=0;
				int closebrs=0;
				while(xml.FindChildElem())
				{
					wstring ws=xml.GetChildTagName();
					string s(ws.begin(),ws.end());
					wstring ws_data=xml.GetChildData();
					string s_data(ws_data.begin(),ws_data.end());
					
					if(s=="COLUMN_NAME")
						{
						 
							int pos=s_data.find_first_of('.')+1;
							Attribute* att=new Attribute(s_data.substr(0,pos-1),s_data.substr(pos));
							Factor *f=new Factor(Factor::ATTRIBUTE,att);
							if(count==0)
							{
								left->PushFactors(f);
								left->ExpressionString.append("A");
							}
							else
							{
								right->PushFactors(f);
								right->ExpressionString.append("A");
							}
						}
					if(s=="LITERAL")
						{
						 
							litVal* val=new litVal();
							val->SetVal(s_data.c_str());
							Factor *f=new Factor(Factor::LITERAL,val);
								if(count==0)
								{
								left->PushFactors(f);
								left->ExpressionString.append("L");
								}
							else
							{
								right->PushFactors(f);
								right->ExpressionString.append("L");
							}
						}
					if(s=="Operator")
						{
							ArithmeticOP* aop=new ArithmeticOP();
							 switch(s_data[0])
							 {
							 case '*':aop->SetType(TYPE::MULTIPLY);break;
							 case '+':aop->SetType(TYPE::PLUS);break;
							 case '-':aop->SetType(TYPE::MINUS);break;
							 case '/':aop->SetType(TYPE::DIVIDE);break;
							 }
							Factor *f=new Factor(Factor::OPERATOR,aop);
								if(count==0)
								{
								left->PushFactors(f);
								left->ExpressionString.append("O");
								}
							else
							{
								right->PushFactors(f);
								right->ExpressionString.append("O");
							}

						}
					if(s=="INTEGER")
						{
							intVal* val=new intVal();
							val->SetVal(atoi(s_data.c_str()));
							Factor *f=new Factor(Factor::INTEGER,val);
								if(count==0)
								{
								left->PushFactors(f);
								left->ExpressionString.append("I");
								}
							else
							{
								right->PushFactors(f);
								right->ExpressionString.append("I");
							}
						}
					if(s=="OPENBR")
					{
						ConditionExpr.append("(");
						if(count==0)
							left->ExpressionString.append("(");
						else
							right->ExpressionString.append("(");
						openbrs++;
					}
					if(s=="CLOSEBR")
					{
						ConditionExpr.append(")");
						if(count==0)
							left->ExpressionString.append(")");
						else
							right->ExpressionString.append(")");
						closebrs++;
					}
				
				}
				xml.OutOfElem();
			
					for(int j=0;j<closebrs-openbrs;j++)
					{
						if(count==0)
						{
						left->ExpressionString="("+left->ExpressionString;
						}
						else
						right->ExpressionString="("+right->ExpressionString;
					}

					if(count==0)
						left->GetExpressionString();
					else
						right->GetExpressionString();
				
 
			}
			

			if(ws_exp==L"COMPOP")
			{
				if(xml.GetChildData()==L">")
					middle->SetType(CompOp::GREATER_THAN);
				else if(xml.GetChildData()==L"<")
					middle->SetType(CompOp::LESS_THAN);
				else if(xml.GetChildData()==L"=")
					middle->SetType(CompOp::EQUAL);
				else if(xml.GetChildData()==L">=")
					middle->SetType(CompOp::EQUAL_OR_GREATER_THAN);
				else if(xml.GetChildData()==L"<=")
					middle->SetType(CompOp::EQUAL_OR_LESS_THAN);
				else
					;
				count++;
			}
		}
		xml.OutOfElem();
		CP->SetComparisonPredicate(left,middle,right);
		SC->AddComparisonPredicate(CP);
		SC->SetType(node::SEARCH_CONDITION);
		std::stringstream os;
		os<<CPcount;
		ConditionExpr.append(os.str());
		ConditionMap.insert(pair<string,node*>(os.str(),SC));
		CPcount++;
		}

		else if(ws_srch==L"Logical")
		{
			if(xml.GetChildData()==L"AND")
				ConditionExpr.append("I");
			else if(xml.GetChildData()==L"OR")
				ConditionExpr.append("U");
			else
				printf("\nIncorrect XML format");
		}
		else if(ws_srch==L"OPENBR")
		{
			ConditionExpr.append("(");

		}
		else if(ws_srch==L"CLOSEBR")
		{
			ConditionExpr.append(")");
		}
		else
			printf("\nIncorrect XML format !");
	}

	xml.OutOfElem();
}

cout<<"\nCondition Expression is:"<<ConditionExpr;
node* logicalroot=ParseCondition(ConditionExpr);
returnCondition(logicalroot);

//Improve the query plan now. Push selection down//

for(vector<node*>::iterator it=conditions.begin();it!=conditions.end();)
{
	vector<string>&tempTbl=checksubtree(*it);
	if(tempTbl.size()<=1){
			for(int j=0;j<T.size();j++)
			{
				if(T[j]->GetTblName().compare(*tempTbl.begin())==0)
					T[j]->AddComparisonPredicate((*it));
			}

			if(it!=conditions.end())
			it=conditions.erase(it); 
		}
	else if(tempTbl.size()>1 && tempTbl.size()<=2)
		{
			Pr.AddJoinCondition(*it);

			if(it!=conditions.end())
			it=conditions.erase(it); 
		}
	else
		it++;
	 
}
//Adding tables to product and projections to tables//
for(int j=0;j<T.size();j++)
{
	Pr.AddTable(T[j]);
	Projection* pi=NULL;
	if(P.GetValue(0)->GetColName().compare("*")!=0)
	{
		pi=new Projection();
	for(int i=0;i<P.GetSize();i++)
	{
		if(P.GetValue(i)->GetTblName()==T[j]->GetTblName())
			pi->AddValue(P.GetValue(i));
	}
	for(map<string,node*>::iterator it=ConditionMap.begin();it!=ConditionMap.end();it++)
	{
		int i=it->second->GetComparison()->GetleftExpression()->GetSize();
		for(int k=0;k<i;k++)
		{
			if(it->second->GetComparison()->GetleftExpression()->GetFactor(k)->GetType()==Factor::ATTRIBUTE && it->second->GetComparison()->GetleftExpression()->GetFactor(k)->GetAttribute()->GetTblName()==T[j]->GetTblName())
				pi->AddValue(it->second->GetComparison()->GetleftExpression()->GetFactor(k)->GetAttribute());
		}
		i=it->second->GetComparison()->GetRightExpression()->GetSize();
		for(int k=0;k<i;k++)
		{
			if(it->second->GetComparison()->GetRightExpression()->GetFactor(k)->GetType()==Factor::ATTRIBUTE && it->second->GetComparison()->GetRightExpression()->GetFactor(k)->GetAttribute()->GetTblName()==T[j]->GetTblName())
				pi->AddValue(it->second->GetComparison()->GetRightExpression()->GetFactor(k)->GetAttribute());
		}
	}
	}
	T[j]->AddProjection(pi);
	if(ifDISTINCT)
	T[j]->SetDuplicateElimination();

}

	return;
			
}

node* ParseCondition(string expr)
{
	if(expr.size()==0)
		return NULL;
	deque<node*> _andorstk;
	deque<node*> _conditionstk;
	deque<node*>::iterator it=_andorstk.end();
	node* U;
	node* I;
	int currentpos=0;
	for(int i=0; i<expr.size()-1; i++)
	{
		 switch(expr[i])
		 {
		 case '(':if(expr[i+1]=='('){currentpos=expr.find_last_of(')');
			 currentpos=expr.rfind(')',currentpos-1);
			 _conditionstk.push_back(ParseCondition(expr.substr(i+1,currentpos-i-1))); i=currentpos;}
		 		  if(_conditionstk.size()>=2 && _andorstk.size()>=1){(*(_andorstk.end()-1))->AddChildren(*(_conditionstk.end()-1));(*(_andorstk.end()-1))->AddChildren(*(_conditionstk.end()-2));
			      _conditionstk.pop_back();_conditionstk.pop_back();_conditionstk.push_back(_andorstk[0]);_andorstk.pop_back();}
				 
				  break;
		 case 'U':U=new node();U->SetType(node::UNION);_andorstk.push_back(U);break;
		 case 'I':I=new node();I->SetType(node::INTERSECTION);_andorstk.push_back(I);break;
		 case ')':break;
		 default: string str(1,expr[i]);
			 _conditionstk.push_back(ConditionMap.find(str)->second);if(_conditionstk.size()>=2 && _andorstk.size()>=1){(*(_andorstk.end()-1))->AddChildren(*(_conditionstk.end()-1));(*(_andorstk.end()-1))->AddChildren(*(_conditionstk.end()-2));
			 _conditionstk.pop_back();_conditionstk.pop_back();_conditionstk.push_back(_andorstk[0]);_andorstk.pop_back();}
			 break;
		 }

	}
	return _conditionstk[0];
}

vector<string> checksubtree(node* root)
{
	if(root->GetType()!=node::SEARCH_CONDITION)
	{
		vector<string>tempi;
		for(int i=0;i<root->GetNoChildren();i++)
		{
			 	
			vector<string>temp=checksubtree(root->GetChild(i));
			for(int i=0;i<temp.size();i++)
			{
				int j;
				for(j=0;j<tempi.size();j++)
				{
					if(tempi[j]==temp[i])
						break;
				}
				if(j>=tempi.size())
					tempi.push_back(temp[i]);

			}
		}
		return tempi;
	}
	else
	{
		root->GetComparison()->ifFromOneTable(); 
		return root->GetComparison()->ofTable;
	}
}

void returnCondition(node* root)
{
	if(root==NULL)
		return ;
	if(root->GetType()==node::UNION)
	{
		conditions.push_back(root);
		return;
	}
	for(int i=0;i<root->GetNoChildren();i++)
	{
		returnCondition(root->GetChild(i));				
	}
	if(root->GetType()==node::SEARCH_CONDITION)
		conditions.push_back(root);
	return;
}
