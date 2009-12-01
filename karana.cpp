#include <iostream>
#include <string>
#ifndef _StorageManager_h_
	#include "StorageManager.h"
	#define _StorageManager_h_ 
#endif

using namespace std;

extern int logical2physical();
extern int query2logical(string );
extern int printresults();

int main (int argc, char ** argv)
{
	
	//Handle command-line params for usage etc
	if (argc == 1)
	{
			cout << "\nKarana: Lightweight SQL interpreter: " << endl;
			cout << "---------------------------------------" << endl;
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
			cout << "\nKarana: Lightweight SQL interpreter: " << endl;
			cout << "---------------------------------------" << endl;
			cout << "Usage:\n Login and then enter SQL queries from the prompt $$" << endl;
			cout << "End every query with a delimiter ;" << endl;
			cout << "Do not enter two complete queries in one line." << endl;
			cout << "Type 'quit' to quit the program." << endl;
			cout << "------------------------------------------------" << endl;
		}
	}

	//Instantiate all major objects
	MainMemory mem;


	// Ask for user to input a query
	string line = "", query = "", linepart = "";
	size_t delimPosition;
	//printlogicaltree or printphysicaltree - flags to signify that the user requested to print parse 	//trees for logical or physical tree  
	int printphysicaltree = 0, printlogicaltree = 0;

	while (line != "quit")
	{
		cout << endl << "$$ ";
		getline(cin, line);
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
		query2logical(query);
		
		if (printlogicaltree == 1) { cout << "I am printing the logical tree. " << endl;}
		
		if (printphysicaltree == 1) { cout << "I am printing the physical tree. " << endl;}
		
		logical2physical();

		printresults();
		}
		cout << "------------------------------------------" << endl;
		if (!linepart.empty())
			cout << "Continuing from the line: " << endl << linepart << endl;
	}
	
}