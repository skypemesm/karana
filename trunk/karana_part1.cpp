#ifndef _StorageManager_h_
	#include "StorageManager.h"
	#define _StorageManager_h_ 
#endif
#include <iostream>
#include <string>
#include <exception>
#include <regex>
#include <map>

using namespace std;
using namespace std::tr1;

//Global memory and disk objects
extern MainMemory mem;
extern SchemaManager schemaMgr;

extern int run_query (string , int , int );

//function prototypes
int callLex (string );
int create_table(string , map<string,string>);
int delete_from_table(string, string);
int insert_into_table(string, map<string,string>);

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
			cout << endl;
			cout << "INSERT QUERY:" << endl;
			cout << "Table: " << res[1] << endl;
			cout << "Attributes: " << res[3] << endl;			
			cout << "Values: " << res[7] << endl;
			
			for (int i = 0; i< res.size(); i++)
			{
				cout << i << " :" << res[i] << endl;
			}
			
			bool has_attr = false;
			string attrs = res[3];
			if (attrs.length() > 0)
				has_attr = true;

			//If it has a select query
			if (res[9] == "select" || res[9] == "SELECT")
			{
				// we need to run the select query first
				run_query(res[5],0,0);

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
			cout << endl;
			cout << "DROP TABLE QUERY:" << endl;
			cout << "Table Name: " << res[1] << endl;

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
			cout << endl;
			cout << "SELECT QUERY:" << endl;
			
			return 0;
		}
		else if (regex_match (thisquery.c_str(), res, delete_pattern ))
		{
			//THIS IS A DELETE QUERY
			cout << endl;
			cout << "DELETE QUERY:" << endl;
			cout << "Table Name: " << res[1] << endl;
			cout << "Condition: " << res[3] << endl;

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
	//split the condition accordingly
	vector<string> conditions_or = split_word(condition, "or");
	for (int ii =0; ii < conditions_or.size(); ii++)
	{
		cout << conditions_or[ii] << endl;
	}

	Relation* thisrelation = schemaMgr.getRelation(table_name);
	//Read each tuple from the relation
	int blocks_read = 0, num_blocks_to_read = 0;
	int total_blocks = thisrelation->getNumOfBlocks();
	while (blocks_read < total_blocks)
	{
		if (total_blocks - blocks_read >= NUM_OF_BLOCKS_IN_MEMORY)
		{
			//I can read data into the whole memory
			num_blocks_to_read = NUM_OF_BLOCKS_IN_MEMORY;
		}
		else
		{
			//I have to read the leftover ones to memory
			num_blocks_to_read = total_blocks - blocks_read;
		}
		//read data into memory
		for (int i = 0; i < num_blocks_to_read; i++ )
			{
				thisrelation->readBlockToMemory((blocks_read + i),i);
			}

		//Get all tuples in memory
		vector<Tuple> tuples;
		tuples = mem.getTuples(0,num_blocks_to_read);
		blocks_read += num_blocks_to_read;

		//for every tuple, check if the condition holds
		for (int i = 0; i < tuples.size(); i++)
		{
			tuples[i].printTuple();
		}

		
	}
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

	thisrelation->printRelation();

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
