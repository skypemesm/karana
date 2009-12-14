#include <iostream>
#include <stdio.h>
#include <string>
#ifndef _StorageManager_h_
	#include "StorageManager.h"
	#define _StorageManager_h_ 
#endif
#include "QueryManager.cpp"
#include <ctime>
#include <cstdlib>

using namespace std;

extern int logical2physical();
extern int query2logical(string );
extern string printresults(int,int );

extern vector<string> split (string ,string );
extern string trim(string&);

//Global memory and disk objects
MainMemory mem;
SchemaManager schemaMgr(&mem);
map<string,node*> ConditionMap;
vector<node*>conditions;
Projection P;
vector<Table*>T;
Product Pr;


//Function prototypes
string run_query( string , int , int, int, bool );
int initial_setup();
int final_cleanup();

/**
* Entry point to the Karana Lightweight SQL Interpreter.
*/
int main (int argc, char ** argv)
{
	
	//Handle command-line params for usage etc
	if (argc == 1)
	{
			cout << "\n    Karana: Lightweight SQL interpreter: " << endl;
			cout << "------------------------------------------------" << endl;
			cout << "Usage:\n Login and then enter SQL queries from the prompt $$" << endl;
			cout << "End every query with a delimiter ;" << endl;
			cout << "Do not enter two complete queries in one line." << endl;
			cout << "Type 'quit' to quit the program." << endl;
			cout << "------------------------------------------------" << endl;
		
	}
	else
	{
		if (argv[1] == "--h" )
		{
			cout << "\n    Karana: Lightweight SQL interpreter: " << endl;
			cout << "------------------------------------------------" << endl;
			cout << "Usage:\n Login and then enter SQL queries from the prompt $$" << endl;
			cout << "End every query with a delimiter ;" << endl;
			cout << "Do not enter two complete queries in one line." << endl;
			cout << "Type 'quit' to quit the program." << endl;
			cout << "------------------------------------------------" << endl;
		}
	}

	//Instantiate all major objects
	cout << "The memory now contains " << mem.getMemorySize() << " blocks" << endl << endl;
	setDelay(10);

	//Perform some initial data setup.
	initial_setup();

	// Ask for user to input a query
	string line = "", query = "", linepart = "";
	const char* chars = "\n\t\v\f\r ";
	size_t delimPosition;
	//printlogicaltree or printphysicaltree - flags to signify that the user requested to print parse 	//trees for logical or physical tree  
	int printphysicaltree = 0, printlogicaltree = 0;


	while (line != "quit" && line != "QUIT")
	{
		//Check if there were any unterminated queries from previous line.
		if (!linepart.empty() && linepart.find_first_not_of(chars) != string::npos)
		{
			cout << "Continuing from the line: " << endl << linepart << endl;
			line = linepart + line;
			linepart = "";
		}
		
		line = trim(line);
		delimPosition = line.find_last_of(";");

		if (delimPosition == string::npos)
		{
			linepart = line;
		}
		else if (delimPosition != line.size())
		{
			query = line.substr(0,delimPosition);
			linepart = line.substr(delimPosition+1);
		}
		
		if (!query.empty())
		{
			if (query == "printlogicaltree")
			{
				printlogicaltree = 1;
			}
			else if (query == "printphysicaltree")
			{
				printphysicaltree = 1;
			}
			else
			{
				//Loop for each query
				run_query(query, printlogicaltree, printphysicaltree, 0, false);
			}
			cout << endl << "------------------------------------------" << endl;
			}
		cout << endl << "$$ ";
		query = "";
		getline(cin, line);
		
		
	}
	
	//Perform some final cleanup
	final_cleanup();
}

/**
* Run a particular query
*/
string run_query( string query, int printlogicaltree, int printphysicaltree, int fromInsert, bool is_delete)
{
	int status;
	time_t start = time (NULL);
	resetDIOs();
	status = query2logical(query);
	if ( status == -1) return "-1";
	else if (status != 3) //all queries other than select return status 3
	{
		// THIS IS A SELECT QUERY.
		if (printlogicaltree == 1) { cout << "I am printing the logical tree. " << endl;}
			
		cout << endl;

		if (logical2physical() == -1) return "-1";
			
		if (printphysicaltree == 1) { cout << "I am printing the physical tree. " << endl;}
		
		if (fromInsert)
			return printresults (0,1);
		else if (is_delete)
			printresults(1,0);
		else
			printresults(0,0);
		
	}

	cout << endl << "Total number of disk I/Os: " << getDIOs() << endl << endl;
	
	time_t end = time (NULL);
	printf("%4.4f seconds taken to execute the query\n\n",(float)(end-start)/1000.0);
	//Resetting global variables for next query
	ConditionMap.clear();
	conditions.clear();
	P.clear();
	T.clear();
	Pr.clear();

	return "";
}

/**
* Perform the initial setup of tables and data.
*/
int initial_setup()
{
	cout << endl << "INITIAL SETUP BEGINS........................................." << endl;
	cout << "............................................................." << endl << endl;
	run_query ("create table course (sid int, homework int, project int, exam int, grade str20)",0,0,0,false);
	
	srand(time(NULL));

	string msg;
	char buffer[50], rr[6];
	int r = 0;

	cout << "How many initial random tuples do you want the course table to have? ";
	gets (rr);
	
	r = atoi(rr); 
	if ( r<0 || r > 100000 )
		return -1;

	cout << endl << endl;
	for (int i = 0; i<r; i++)
	{
		msg = "insert into course(sid, homework,grade, project, exam) values(";
		itoa(rand()%200, buffer, 10);
		msg += buffer;
		msg+= ",";
		itoa(rand()%100, buffer, 10);
		msg += buffer;
		msg+= ",\"stt";
		itoa(rand()%10, buffer, 10);
		msg += buffer;
		msg+= "\",";
		itoa(rand()%50, buffer, 10);
		msg += buffer;
		msg+= ",";
		itoa(rand()%5, buffer, 10);
		msg += buffer;
		msg+= ")";
		cout << msg << endl;
		run_query (msg,0,0,0,false);

	}	
	Relation* thisrelation = schemaMgr.getRelation("course");
	thisrelation->printRelation();

	cout << endl << "Initial setup complete.." << endl << endl;
	return 0;
}

/**
* Performs some final schema and data cleanup.
*/
int final_cleanup()
{
	cout << "........................................................" << endl;
	run_query ("delete from course",0,0,0,false);
	run_query("drop table course",0,0,0,false);
	cout << endl << "Final cleanups complete. press ENTER to exit." << endl;

	string r;
	getline(cin,r);
	return 0;
}
