#include <iostream>
#include <string>
#ifndef _StorageManager_h_
	#include "StorageManager.h"
	#define _StorageManager_h_ 
#endif

using namespace std;


int logical2physical();
int printresults();
void parse (string );
void cost_calculation();
void order_join ();
void run_operations();

// The main logic. 
int logical2physical()
{
	cout << "Entering phase 2: To convert logical plan to physical plan." << endl;

	//Parse the logical file and store it in arrays
	parse ("logical.xml");

	//calculate the costs
	cost_calculation();

	//Find the join order
	order_join();

	//Run the actual sequence of operations
	run_operations();

	return 0;
}

void parse (string filename)
{
	//Open the XML file

	//Read it node by node

	//validate it

	//Store in arrays

}

void cost_calculation()
{
	
}

void order_join ()
{
	
}

void run_operations()
{
	
}

int printresults()
{
	return 0;
}