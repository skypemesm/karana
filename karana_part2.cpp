#include <iostream>
#include <string>
#include "StorageManager.h"
#include "QueryManager.cpp"
#include <algorithm>
#include <typeinfo>
#include <map>
#include <set>
using namespace std;


//Global memory and disk objects
extern MainMemory mem;
extern SchemaManager schemaMgr;
extern map<string,node*> ConditionMap;
extern vector<node*>conditions;
extern Projection P;
extern vector<Table*>T;
extern Product Pr;
extern vector<string> split (string ,string );
extern string trim(string&);
extern Attribute* ifOrderBy;

//Function prototypes
int logical2physical();
string printresults(int ,int );
void cost_calculation();
void order_join ();
void run_operations();
vector <Tuple> sort_by (string , vector<string> );
string twopassDupElim(string );
bool twopassJoin(string ,string ,node* ,vector<vector<Tuple>*>& );
void ksort(vector <Tuple> &array, vector <int >, vector<string>);
void quickSort(vector <Tuple> &arr, int , int , vector <int >, vector<string>);
int tuple_comp(Tuple , Tuple ,  vector <int >, vector<string>);
bool areTuplesEqual(Tuple , Tuple);
int GetFirstFreeMemBlock();
bool GetTupleVal(Tuple& , node* , string );
vector<string> checksubtree(node* );
string onepass_selection(string , node* );
string onepass_projection(string , Projection* , bool );
string RemoveDuplicates(string );
bool JoinRelations(Tuple& , string , Tuple& , string , node* , SchemaManager );
bool Join_onepass(string ,string ,node* ,vector<vector<Tuple>*>& );
void CreateDebugData();
string ExecuteQuery(int );
Relation* CreateSchema( Schema* thisschema, string tablename);

 

/**
* This function converts the logical tree to a physical plan.
*/
int logical2physical()
{
	cout << "Entering phase 2: To convert logical plan to physical plan." << endl;

	//calculate the costs of various subtrees
	cost_calculation();

	//Find the join order
	order_join();

	//Run the actual sequence of operations
	run_operations();

	return 0;
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

/**
* This method prints the results for a query.
*/
string printresults(int is_delete=0, int insert = 0)
{

	string result_table = ExecuteQuery(is_delete);

	if(insert)
		return result_table;
	else
		return "";
	/*
	//Testing DISTINCT	
	vector <Tuple> results = twopassDupElim("course");	
	if ( results.empty())
		return -1;
	else
	{
		//Print the tuples.
		for (int i = 0; i<results.size(); i++)
			results[i].printTuple();
	}

	//Testing ORDER BY
	vector<string > columnNames;
	
	columnNames.push_back("sid");
	columnNames.push_back("grade");
	vector <Tuple> results1 = sort_by("course", columnNames);	
	if ( results1.empty())
		return -1;
	else
	{
		//Print the tuples.
		for (int i = 0; i<results1.size(); i++)
			results1[i].printTuple();
	}
	*/
}

/**
* Sorts the data according to given columns.
*/

vector <Tuple> sort_by (string relationname, vector<string> columns)
{
	vector<Tuple> results, tuples_in_memory;
	Relation* thisrelation = schemaMgr.getRelation (relationname);
	Schema* thisschema = schemaMgr.getSchema (relationname);

	vector<int > columnPositions;
	vector <string> columnTypes;
	//find the columnPositions
	for (int i = 0; i< columns.size(); i++)
	{
		columnPositions.push_back(thisschema->getFieldPos(columns[i]));
		columnTypes.push_back(thisschema->getFieldType(columns[i]));
	}

	Schema tempSchema (columns, columnTypes);	
	Relation* temprelation = schemaMgr.createRelation("temp1", tempSchema);

	int total_blocks = thisrelation->getNumOfBlocks();
	int blocks_read = 0, num_blocks_to_read = 0, no_of_buckets = 0;
	
	// First Pass:
	// Read all blocks into memory and sort them
	while (blocks_read < total_blocks) 
	{
		no_of_buckets ++;
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
		
		tuples_in_memory = mem.getTuples(0,num_blocks_to_read);
		//sort them
		ksort (tuples_in_memory, columnPositions, columnTypes);
		mem.setTuples(0, tuples_in_memory);

		//write these blocks back to disk
		for (int i = 0; i < num_blocks_to_read; i++ )
			{
				thisrelation->writeBlockFromMemory((blocks_read + i),i);
				temprelation->writeBlockFromMemory((blocks_read + i),i);
				
			}

		blocks_read += num_blocks_to_read;
		num_blocks_to_read = 0;
	}
	
	//mem.dumpMemory();

	// Second Pass:
	
	if (no_of_buckets <= NUM_OF_BLOCKS_IN_MEMORY)
	{
		vector <int> qPositions(no_of_buckets);
 		
		int round = 0;
		//Loop
		while (round < NUM_OF_BLOCKS_IN_MEMORY)
		{
		//We can get first blocks of all the buckets
		for (int i = 0; i < no_of_buckets ; i++)
		{
			thisrelation->readBlockToMemory(i * NUM_OF_BLOCKS_IN_MEMORY + round, i);
			qPositions[i] = 0;
		}

		vector <Tuple> curr_tuples = mem.getTuples(0, no_of_buckets);
		vector <Tuple>::iterator it, itr;
		int no_push = 0;
		// Return sorted tuples
		for (it=curr_tuples.begin(); it!= curr_tuples.end(); it++)
		{
			for (itr = results.begin(); itr!=results.end(); itr++)
			{
				if (areTuplesEqual(*it, *itr))
				{no_push = 1; break;}
			}
			results.push_back(*it);
			no_push = 0;
		}
		round++;
		}			

		ksort (results, columnPositions, columnTypes);
	}
	else
	{
		//We cannot get the first blocks of all the buckets
		cerr << "Sorry the table is just too large. B(R) > M^2. We cannot do a two pass merge sort."
			<< "Please use indexes on this table." << endl;
	}
	return results;

}

/**
* This function is used to eliminate the duplicates in a relation.
*/
string twopassDupElim(string relationname)
{
	vector<Tuple> results, tuples_in_memory;
	Relation* thisrelation = schemaMgr.getRelation (relationname);
	Schema* thisschema = schemaMgr.getSchema (relationname);
	string newRel=relationname + "_dupremoved"; 
	Relation* newrel=CreateSchema(schemaMgr.getSchema(relationname),newRel);
	Schema* newschema = schemaMgr.getSchema(newRel);
	
	vector<string> columns = thisschema->getAllColumnNames();
	vector<int > columnPositions;
	vector <string> columnTypes;
	//find the columnPositions
	for (int i = 0; i< columns.size(); i++)
	{
		columnPositions.push_back(thisschema->getFieldPos(columns[i]));
		columnTypes.push_back(thisschema->getFieldType(columns[i]));
	}

	Schema tempSchema (columns, columnTypes);	
	Relation* temprelation = schemaMgr.createRelation("temp1", tempSchema);

	int total_blocks = thisrelation->getNumOfBlocks();
	int blocks_read = 0, num_blocks_to_read = 0, no_of_buckets = 0;
	
	// First Pass:
	// Read all blocks into memory and sort them
	while (blocks_read < total_blocks) 
	{
		no_of_buckets ++;
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
		
		tuples_in_memory = mem.getTuples(0,num_blocks_to_read);
		//sort them
		ksort (tuples_in_memory, columnPositions, columnTypes);
		mem.setTuples(0, tuples_in_memory);

		//write these blocks back to disk
		for (int i = 0; i < num_blocks_to_read; i++ )
			{
				temprelation->writeBlockFromMemory((blocks_read + i),i);
			}

		blocks_read += num_blocks_to_read;
		num_blocks_to_read = 0;
	}
	
	//mem.dumpMemory();

	// Second Pass:

	if (no_of_buckets <= NUM_OF_BLOCKS_IN_MEMORY)
	{
		vector <int> qPositions(no_of_buckets);
 		
		int round = 0;
		//Loop
		while (round < NUM_OF_BLOCKS_IN_MEMORY)
		{
		//We can get first blocks of all the buckets
		for (int i = 0; i < no_of_buckets ; i++)
		{
			thisrelation->readBlockToMemory(i * NUM_OF_BLOCKS_IN_MEMORY + round, i);
			qPositions[i] = 0;
		}

		vector <Tuple> curr_tuples = mem.getTuples(0, no_of_buckets);
		vector <Tuple>::iterator it, itr;
		int no_push = 0;
		// Add non-duplicate tuples
		for (it=curr_tuples.begin(); it!= curr_tuples.end(); it++)
		{
			for (itr = results.begin(); itr!=results.end(); itr++)
			{
				if (areTuplesEqual(*it, *itr))
				{no_push = 1; break;}
			}
			if(no_push == 0)
			{
				(*it).setSchema(newschema);
				results.push_back(*it);
			}
			no_push = 0;
		}
		round++;
		}			

		ksort (results, columnPositions, columnTypes);

		blocks_read = 0;
		int count = 0;
		for (vector <Tuple>::iterator it = results.begin();it !=results.end(); it++)
		{
			if (mem.getBlock(0)->isFull())
			{
				newrel->writeBlockFromMemory(count++,0);
				mem.getBlock(0)->clear();
			}
			mem.getBlock(0)->appendTuple(*it);

		}

	}
	else
	{
		//We cannot get the first blocks of all the buckets
		cerr << "Sorry the table is just too large. B(R) > M^2. We cannot do a two pass merge sort."
			<< "Please use indexes on this table." << endl;
	}
	return newRel;
	
}

/**
* Implements a two pass Join algorithm.
*/
bool twopassJoin(string tableA,string tableB,node* condition,vector<vector<Tuple>*>& _tuplepair)
{
	vector<Tuple> resultsA, resultsB;
	Relation* relationA = schemaMgr.getRelation (tableA);
	Schema* schemaA = schemaMgr.getSchema (tableA);
	Relation* relationB = schemaMgr.getRelation (tableB);
	Schema* schemaB = schemaMgr.getSchema (tableB);
	
	//find the smaller relation of the two
	if (relationA->getNumOfTuples() > relationA->getNumOfTuples())
	{string temp;
	 temp = tableB;
	 tableB=tableA;
	 tableA=temp;
	 Relation* relationA = schemaMgr.getRelation (tableA);
	 Schema* schemaA = schemaMgr.getSchema (tableA);
	 Relation* relationB = schemaMgr.getRelation (tableB);
	 Schema* schemaB = schemaMgr.getSchema (tableB);
	}
	vector<string> columnsA = schemaA->getAllColumnNames();
	vector<string> columnsB = schemaB->getAllColumnNames();

	vector<int > columnPositionsA;
	vector <string> columnTypesA;
	//find the columnPositions
	for (int i = 0; i< columnsA.size(); i++)
	{
		columnPositionsA.push_back(schemaA->getFieldPos(columnsA[i]));
		columnTypesA.push_back(schemaA->getFieldType(columnsA[i]));
	}

	Schema tempSchemaA (columnsA, columnTypesA);	
	Relation* temprelation = schemaMgr.createRelation("temp1A", tempSchemaA);

	vector<int > columnPositionsB;
	vector <string> columnTypesB;
	//find the columnPositions
	for (int i = 0; i< columnsB.size(); i++)
	{
		columnPositionsB.push_back(schemaB->getFieldPos(columnsB[i]));
		columnTypesB.push_back(schemaB->getFieldType(columnsB[i]));
	}

	Schema tempSchemaB (columnsB, columnTypesB);	
	Relation* temprelationB = schemaMgr.createRelation("temp1B", tempSchemaB);

	//Sort A
	resultsA = sort_by(tableA, columnsA);

	//Sort B
	resultsB = sort_by(tableB, columnsB);
	
	//Write the blocks to memory
	mem.setTuples(0,resultsB);
	for (int i=0;i<resultsB.size();i++)
	{temprelationB->writeBlockFromMemory(i,i);}

	//Merge the sorted results
	for (vector <Tuple>::iterator it = resultsA.begin(); it != resultsA.end(); it++)
	{
		for (vector <Tuple>::iterator itr = resultsB.begin(); it != resultsB.end(); itr++)
		{
			itr->setSchema(schemaMgr.getSchema(temprelationB->getRelationName()));
			it->setSchema(schemaMgr.getSchema(temprelation->getRelationName()));
			vector<Tuple>*temp=new vector<Tuple>();
			temp->push_back(*itr);
			temp->push_back(*it);
			_tuplepair.push_back(temp);
		}
	}

	for(int i=0;i<temprelation->getNumOfBlocks();i++)
		mem.getBlock(i)->clear();

	return true;
}

/**
* Implements the Sort physical function.
* @param vector of column positions which are to be used for sorting
* @param type of sort columns
*/
void ksort(vector <Tuple> &array, vector <int >sortColumnPositions, vector<string> sortColumnTypes)
{
		quickSort(array, 0, array.size()-1, sortColumnPositions, sortColumnTypes);
}

void quickSort(vector <Tuple> &arr, int left, int right, vector<int > sortColumnPositions, 	vector<string>  sortColumnTypes)
{
   int i = left, j = right;
   Tuple tmp = arr[left];
   Tuple pivot = arr[(left + right) / 2];
 
      /* partition */
   while (i <= j) {
	   while (tuple_comp(arr[i], pivot, sortColumnPositions, sortColumnTypes) == -1)
                  i++;
         while (tuple_comp(arr[j], pivot, sortColumnPositions, sortColumnTypes) == 1)
                  j--;
       if (i <= j) {
                  tmp = arr[i];
                  arr[i] = arr[j];
                  arr[j] = tmp;
                  i++;
                  j--;
            }
      }
 
      /* recursion */
      if (left < j)
		  quickSort (arr, left, j, sortColumnPositions, sortColumnTypes);
      if (i < right)
            quickSort (arr, i, right, sortColumnPositions, sortColumnTypes);
}


// Compares two tuples. returns 1 if left is greater than right,0 if equal or else returns -1
int tuple_comp (Tuple left, Tuple right, vector<int> sortColumnPositions, vector<string> sortColumnTypes)
{
	int i = 0, diff = 0;
	while (i < sortColumnTypes.size())
	{
		if (sortColumnTypes[i] == "INT")
		{
			if (left.getInt(sortColumnPositions[i]) > right.getInt(sortColumnPositions[i]))
				{
					return 1;
				}
				else if (left.getInt(sortColumnPositions[i]) == right.getInt(sortColumnPositions[i]))
				{
					i++;
				}
				else
				{
					return -1;
				}
			
		}
		else
		{
			diff = left.getString(sortColumnPositions[i]).compare( 
				 					right.getString(sortColumnPositions[i]) );
			 if (diff == 0)
				 i++;
			 else 
				 return diff;
		}
	}
	return 0;
}

/**
* Returns true if the functions are equal, else false.
*/
bool areTuplesEqual(Tuple left,Tuple right)
{
		if (left.getNumOfInts() != right.getNumOfInts()
			|| left.getNumOfStrings() != right.getNumOfStrings())
		{
			int leftTotalFields = left.getNumOfInts() + left.getNumOfStrings();
			int rightTotalFields = right.getNumOfInts() + right.getNumOfStrings();
			return leftTotalFields < rightTotalFields;
		}

		for (int i = 0; i< left.getNumOfInts(); i++)
		{
			if (left.getInt(i) != right.getInt(i) )
				return false;
		}
		for (int i = 0; i< left.getNumOfStrings(); i++)
		{
			if (left.getString(i).compare(right.getString(i)) != 0 )
				return false;
		}
				
		return true;

}
 
string onepass_selection(string relation, node* condition)
{
	Relation* rel=schemaMgr.getRelation(relation);
	string newRel=relation + "_seltemp"; 
	Relation* newrel=CreateSchema(schemaMgr.getSchema(relation),newRel);//SchemaMgr.createRelation(newRel,*schemaMgr.getSchema(relation));
	Schema* newschema = schemaMgr.getSchema(newRel);	
	int free_mem_blocks=0;
	int first_free_mem_index=GetFirstFreeMemBlock();
	if(first_free_mem_index==-1)
		return "null";
	int second_free_mem_index=GetFirstFreeMemBlock() + 1;
	if(second_free_mem_index==-1)
		return "null";
	//if((rel=schemaMgr.getRelation(relation))==NULL)
	//	return -1;

	//if(free_mem_blocks>rel->getNumOfBlocks())
	//	return -2;
	 
		for(int i=0;i<rel->getNumOfBlocks();i++)
		{
			if(schemaMgr.getRelation(relation)->readBlockToMemory(i,first_free_mem_index)==true)
			{
				vector<Tuple>& temp=mem.getBlock(first_free_mem_index)->getTuples();
				for(vector<Tuple>::iterator it=temp.begin();it!=temp.end();it++)
				{
					if(GetTupleVal(*it,condition,relation)==true)
					{
						if(mem.getBlock(second_free_mem_index)->isFull())
						{
							newrel->writeBlockFromMemory(newrel->getNumOfBlocks(),second_free_mem_index);
							mem.getBlock(second_free_mem_index)->clear();
						}

						(*it).setSchema(newschema);
						mem.getBlock(second_free_mem_index)->appendTuple(*it);

					}

				}
				
			}
			mem.getBlock(first_free_mem_index)->clear();
		}

		if(mem.getBlock(second_free_mem_index)->getNumTuples()>0)
		newrel->writeBlockFromMemory(newrel->getNumOfBlocks(),second_free_mem_index);
		mem.getBlock(second_free_mem_index)->clear();
		return newRel;

}

 bool GetTupleVal(Tuple& t, node* condition, string relName)
 {
	 if(condition->GetType()==node::SEARCH_CONDITION)
	 return condition->GetComparison()->ComputeExpression(t,schemaMgr,relName);
	 else if(condition->GetType()==node::INTERSECTION)
	 {
		 return GetTupleVal(t,condition->GetChild(0),relName) && GetTupleVal(t,condition->GetChild(1),relName);
	 }
	 else if(condition->GetType()==node::UNION)
	 {
		 return GetTupleVal(t,condition->GetChild(0),relName) || GetTupleVal(t,condition->GetChild(1),relName);
	 }
 }
//for standard output from Relation
string onepass_projection(string relation, Projection* p, bool ifPrint)
{
	if(p==NULL)
		return relation;
	vector<string> fieldnames;
	vector<string> fieldtypes;
	Relation* rel=schemaMgr.getRelation(relation);
	Schema* relschema=schemaMgr.getSchema(relation);
	string newRel=relation + "_proj";
 
		for(int i=0;i<p->GetSize();i++)
		{
			fieldnames.push_back(p->GetValue(i)->GetColName());
			fieldtypes.push_back(relschema->getFieldType(p->GetValue(i)->GetColName()));

		}
	Schema newrelscm(fieldnames,fieldtypes);
	Relation* newrel=schemaMgr.createRelation(newRel,newrelscm);
	int free_mem_blocks=0;
	int first_free_mem_index=GetFirstFreeMemBlock();
	if(first_free_mem_index==-1)
		return "null";
	int second_free_mem_index=first_free_mem_index+1;
	if(second_free_mem_index==-1)
		return "null";
	
	for(int i=0;i<rel->getNumOfBlocks();i++)
		{
			if(schemaMgr.getRelation(relation)->readBlockToMemory(i,first_free_mem_index)==true)
			{
				vector<Tuple>& temp=mem.getBlock(first_free_mem_index)->getTuples();
				for(vector<Tuple>::iterator it=temp.begin();it!=temp.end();it++)
				{
					Tuple t(schemaMgr.getSchema(newRel));
					for(int j=0;j<p->GetSize();j++)
					{
						int pos=newrelscm.getFieldPos(p->GetValue(j)->GetColName());
						if(newrelscm.getFieldType(p->GetValue(j)->GetColName()).compare("INT")==0)
						t.setField(pos,it->getInt(pos));
						else
						t.setField(pos,it->getString(pos));
					}
					if(!ifPrint)
					{
					if(mem.getBlock(second_free_mem_index)->isFull())
						{
							newrel->writeBlockFromMemory(newrel->getNumOfBlocks(),second_free_mem_index);
							mem.getBlock(second_free_mem_index)->clear();
						}

						mem.getBlock(second_free_mem_index)->appendTuple(t);
					}
					else
					{
						t.printTuple();
					}
				}
			}
			mem.getBlock(first_free_mem_index)->clear();
		}
	if(!ifPrint)
	{
	if(mem.getBlock(second_free_mem_index)->getNumTuples()>0)
	newrel->writeBlockFromMemory(newrel->getNumOfBlocks(),second_free_mem_index);
	mem.getBlock(second_free_mem_index)->clear();
	return newRel;
	}
	else
		return "null";

}

int GetFirstFreeMemBlock()
{
	int index=-1;
	for(int i=0;i<mem.getMemorySize();i++)
	{
		if(mem.getBlock(i)->isFull()==false)
		{
			index=i;
			break;
		}
	}
	return index;
}


string RemoveDuplicates(string relation)
{
vector <Tuple> results;
Relation* rel=schemaMgr.getRelation(relation);
string newRel=relation + "_dupremoved"; 
Relation* newrel=CreateSchema(schemaMgr.getSchema(relation),newRel);
Schema* newschema = schemaMgr.getSchema(newRel);
int free_mem_blocks=0;
int first_free_mem_index=GetFirstFreeMemBlock();
if(first_free_mem_index==-1)
return "null";
int second_free_mem_index=GetFirstFreeMemBlock() + 1;
if(second_free_mem_index==-1)
return "null";
int no_push = 0;
for(int i=0;i<rel->getNumOfBlocks();i++)
		{
			if(schemaMgr.getRelation(relation)->readBlockToMemory(i,first_free_mem_index)==true)
			{
				vector<Tuple>& temp=mem.getBlock(first_free_mem_index)->getTuples();
				for(vector<Tuple>::iterator it=temp.begin();it!=temp.end();it++)
				{
					for (vector<Tuple>::iterator itr = results.begin(); itr!=results.end(); itr++)
					{
						if (areTuplesEqual(*it, *itr))
							{no_push = 1; break;}
					}
					if(no_push == 0)
					{
						(*it).setSchema(newschema);
						results.push_back(*it);
					}
					no_push = 0;
				}			
			}

			mem.getBlock(first_free_mem_index)->clear();

		}

second_free_mem_index=GetFirstFreeMemBlock();
int reqsize;
int tuples_per_block = schemaMgr.getSchema(relation)->getTuplesPerBlock();
reqsize=results.size()%tuples_per_block ==0 ? results.size()/tuples_per_block:results.size()/tuples_per_block+1;
if(reqsize > mem.getMemorySize()-1)
	return "null";
 
mem.setTuples(second_free_mem_index,results);
for(int k=0;k<reqsize;k++)
{
	newrel->writeBlockFromMemory(newrel->getNumOfBlocks(),second_free_mem_index+k);
	mem.getBlock(second_free_mem_index+k)->clear();
}
return newRel;
}

bool JoinRelations(Tuple& tA, string relA, Tuple& tB, string relB, node* condition, SchemaManager sm)
{
	if(condition==NULL)
		return true;
	if(condition->GetType()==node::INTERSECTION)
		return JoinRelations(tA,relA,tB,relB,condition->GetChild(0),sm) && JoinRelations(tA,relA,tB,relB,condition->GetChild(1),sm);
	else if(condition->GetType()==node::UNION)
		return JoinRelations(tA,relA,tB,relB,condition->GetChild(0),sm) || JoinRelations(tA,relA,tB,relB,condition->GetChild(1),sm);
	else
	{
		if(condition->GetComparison()->ofTable.size()<=1)
		{
			if(relB.compare(*condition->GetComparison()->ofTable.begin())==0)
			return condition->GetComparison()->ComputeExpression(tB,sm,relB);
			else
			return condition->GetComparison()->ComputeExpression(tB,sm,relB);
		}
		else
			return condition->GetComparison()->ComputeJoin(tA,relA,tB,relB,sm);

	}
}
//always print the output.returns false if onepass join is not possible//
bool Join_onepass(string tableA,string tableB,node* condition,vector<vector<Tuple>*>& _tuplepair)
{
	vector<string> fieldnames;
	vector<string> fieldtypes;
	Relation* smallrel; 
	Relation* bigrel; 
	if(schemaMgr.getRelation(tableA)->getNumOfBlocks()<schemaMgr.getRelation(tableB)->getNumOfBlocks())
	{smallrel=schemaMgr.getRelation(tableA);
	bigrel=schemaMgr.getRelation(tableB);}
	else
	{bigrel=schemaMgr.getRelation(tableA);
	smallrel=schemaMgr.getRelation(tableB);
	}

	Schema* smallrelschm=schemaMgr.getSchema(smallrel->getRelationName());
	Schema* bigrelschm=schemaMgr.getSchema(bigrel->getRelationName());
	
	if(1+smallrel->getNumOfBlocks()>mem.getMemorySize())
		return false;
	
	int first_free_mem_index=GetFirstFreeMemBlock();
	if(first_free_mem_index==-1)
	return false;
 	int s_index=first_free_mem_index;
	for(int i=0;i<smallrel->getNumOfBlocks();i++)
	{
		smallrel->readBlockToMemory(i,first_free_mem_index+i);
	}
	first_free_mem_index=GetFirstFreeMemBlock() +1 ;
	for(int j=0;j<bigrel->getNumOfBlocks();j++)
	{
		if(bigrel->readBlockToMemory(j,first_free_mem_index)==true)
			{
				vector<Tuple>& bigtemp=mem.getBlock(first_free_mem_index)->getTuples();
				for(vector<Tuple>::iterator it=bigtemp.begin();it!=bigtemp.end();it++)
				{
					for(int k=0;k<smallrel->getNumOfBlocks();k++)
					{
						vector<Tuple>& smalltemp=mem.getBlock(s_index+k)->getTuples();
						for(vector<Tuple>::iterator sit=smalltemp.begin();sit!=smalltemp.end();sit++)
						{
							if(JoinRelations(*sit,smallrel->getRelationName(),*it,bigrel->getRelationName(),condition,schemaMgr))
							{
								vector<Tuple>*temp=new vector<Tuple>();
								sit->setSchema(smallrelschm);
								it->setSchema(bigrelschm);
								temp->push_back(*sit);
								temp->push_back(*it);
								_tuplepair.push_back(temp);
								//smallrelschm->printSchema();bigrelschm->printSchema();printf("\n");
								//sit->printTuple();it->printTuple();
							}

						}
					}

				}
			}
		mem.getBlock(first_free_mem_index)->clear();

	}

	for(int i=0;i<smallrel->getNumOfBlocks();i++)
		mem.getBlock(s_index+i)->clear();

	return true;
}

string ExecuteQuery(int is_delete = 0)
{
	vector<Table*>&tables=Pr.GetTables();
	vector<node*>&conditions=Pr.GetConditions();
	vector<string>tblnames;
	for(int i=0;i<tables.size();i++)
	{
		string tab=tables[i]->GetTblName();
		for(int j=0;j<tables[i]->getnoConditions();j++)
		{
			tab=onepass_selection(tab,tables[i]->GetComparisonPredicate(j));
		}
		tab=onepass_projection(tab,tables[i]->GetProjection(),false);
		if(tables[i]->GetDuplicateElimination())
		{
		string tab1;
		if((tab1=RemoveDuplicates(tab))=="null")
		{
			//call 2 pass duplicate elimination
			tab = twopassDupElim(tab);
		}
		else
		{
			tab=tab1;
		}
		}
		tblnames.push_back(tab);
	}

	if(tables.size()>2)
	{
		;//multi table join 
	}
	else if(tables.size()>1 && tables.size()<=2)
	{
		vector<vector<Tuple>*>tuplepair;
		if(Join_onepass(tblnames[0],tblnames[1],Pr.Getrootnode(),tuplepair)==false)
		{
			//2 pass join required
			tuplepair.clear();
			twopassJoin(tblnames[0],tblnames[1],Pr.Getrootnode(),tuplepair);
		}
		else
		{
			printf("\n****************PRINTING RESULT*****************\n");
			int icnt=0;
			(*tuplepair[0])[0].getSchema()->printSchema();
			printf("\t");
			(*tuplepair[0])[1].getSchema()->printSchema();
			printf("\n");
			for(int i=0;i<tuplepair.size();i++)
			{
				for(int j=0;j<tuplepair[i]->size();j++)
				{
					(*tuplepair[i])[j].printTuple();
					printf("\t");
				}
				icnt++;
				printf("\n");
			}
			printf("\n %d results found",icnt);
		}
		//2 table join
	}
	else
	{
		if (is_delete)
		{
			//Need to delete the tuples.
			cout << tblnames[0] << endl;
			vector <Tuple> temp, tempold, tempnew;

			Relation* delrel = schemaMgr.getRelation(tblnames[0]);
			Relation* rel = schemaMgr.getRelation(tables[0]->GetTblName());

			if(delrel->getNumOfTuples()<=0)
				printf("\n 0 rows deleted.");
			else
			{
			int total_blocks = rel->getNumOfBlocks();
			int blocks_read = 0, num_blocks_to_read = 0;
			

			// Read all blocks into memory
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
						rel->readBlockToMemory((blocks_read + i),i);
					}
				
				temp = mem.getTuples(0,num_blocks_to_read);
				for ( int i=0; i< temp.size(); i++)
					tempold.push_back(temp[i]);

				blocks_read += num_blocks_to_read;
				num_blocks_to_read = 0;
			}

			total_blocks = delrel->getNumOfBlocks();
			blocks_read = 0; num_blocks_to_read = 0;

			// Read all blocks into memory 
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
						delrel->readBlockToMemory((blocks_read + i),i);
					}
				
				temp = mem.getTuples(0,num_blocks_to_read);
				for ( int i=0; i< temp.size(); i++)
					tempnew.push_back(temp[i]);

				blocks_read += num_blocks_to_read;
				num_blocks_to_read = 0;
			}
			temp.clear();
			rel->deleteBlock(0);
			int no_add = 0;
			for ( vector<Tuple>::iterator it = tempold.begin();it != tempold.end(); it++)
			{
				for (vector <Tuple>::iterator itr = tempnew.begin(); itr != tempnew.end(); itr++)
				{
					if( areTuplesEqual(*it,*itr) )
					{no_add =1;break;}
				}
				if(no_add == 0)
					temp.push_back(*it);
				no_add = 0;
			}

			int i = 0, count = 0;
			while (i<temp.size())
			{
				for (int j=0; j<NUM_OF_BLOCKS_IN_MEMORY; j++)
				{
					mem.getBlock(0)->appendTuple(temp[i]);
					if(mem.getBlock(0)->isFull())
					{
						rel->writeBlockFromMemory(0,count++);
						mem.getBlock(0)->clear();
					}
				
				}
				i=+ NUM_OF_BLOCKS_IN_MEMORY;
			}
			}

			cout << tempnew.size() <<" Rows deleted successfully." << endl;

		}
		else
		{

			printf("\n****************PRINTING RESULT*****************\n");
			Relation* rel =schemaMgr.getRelation(tblnames[0]);
			if(rel->getNumOfTuples()<=0)
				printf("\n 0 rows found");
			else
			rel->printRelation();
			 
			;//no join
		}
	}

	return tblnames[0];
}

Relation* CreateSchema( Schema* thisschema, string tablename)
 {
	vector<string> columns = thisschema->getAllColumnNames();
	vector<int > columnPositions;
	vector <string> columnTypes;
//find the columnPositions
	for (int i = 0; i< columns.size(); i++)
	{
		columnPositions.push_back(thisschema->getFieldPos(columns[i]));
		columnTypes.push_back(thisschema->getFieldType(columns[i]));
	}

	Schema tempSchema (columns, columnTypes);	

	Relation* temprelation = schemaMgr.createRelation(tablename, tempSchema);

	return temprelation;
 }

void CreateDebugData()
{
	// Create a schema
  vector<string> fieldNames;
  vector<string> fieldTypes;
  fieldNames.push_back("f1");
  fieldNames.push_back("f2");
  fieldNames.push_back("f3");
  fieldNames.push_back("f4");
  fieldNames.push_back("f5");
  fieldTypes.push_back("STR20");
  fieldTypes.push_back("STR20");
  fieldTypes.push_back("INT");
  fieldTypes.push_back("STR20");
  fieldTypes.push_back("INT");
  Schema schema1(fieldNames,fieldTypes);

   // Create a relation through the schema manager
  string relationName1="sd";
  Relation* relationPtr1=schemaMgr.createRelation(relationName1,schema1);

  fieldNames.clear();
  fieldTypes.clear();
  fieldNames.push_back("f1");
  fieldNames.push_back("f2");
  fieldNames.push_back("f3");
  fieldNames.push_back("f4");
  fieldNames.push_back("f5");
  fieldTypes.push_back("STR20");
  fieldTypes.push_back("STR20");
  fieldTypes.push_back("INT");
  fieldTypes.push_back("STR20");
  fieldTypes.push_back("INT");
  Schema schema2(fieldNames,fieldTypes);

   // Create a relation through the schema manager
  string relationName2="ds";
  Relation* relationPtr2=schemaMgr.createRelation(relationName2,schema2);

    // Set up a block in the memory
  Tuple tuple(&schema1);
  Block* block=mem.getBlock(0); //access to memory block 0

  cout << "Insert the following tuples into memory block 0" << endl;
  block->clear(); //clear the block

  // Set up the first tuple
  tuple.setField(schema1.getFieldPos("f1"),"v11");
  tuple.setField(schema1.getFieldPos("f2"),"v21");
  tuple.setField(schema1.getFieldPos("f3"),31);
  tuple.setField(schema1.getFieldPos("f4"),"v41");
  tuple.setField(schema1.getFieldPos("f5"),51);
  tuple.printTuple();
  block->appendTuple(tuple);

  // the second tuple
  tuple.setField(schema1.getFieldPos("f1"),"v11");
  tuple.setField(schema1.getFieldPos("f2"),"v21");
  tuple.setField(schema1.getFieldPos("f3"),31);
  tuple.setField(schema1.getFieldPos("f4"),"v41");
  tuple.setField(schema1.getFieldPos("f5"),51);
  tuple.printTuple();
  block->appendTuple(tuple);

  // the third tuple
  tuple.setField(schema1.getFieldPos("f1"),"v13");
  tuple.setField(schema1.getFieldPos("f2"),"v23");
  tuple.setField(schema1.getFieldPos("f3"),33);
  tuple.setField(schema1.getFieldPos("f4"),"v43");
  tuple.setField(schema1.getFieldPos("f5"),53);
  tuple.printTuple();
  block->appendTuple(tuple);

  // the four tuple
  tuple.setField(schema1.getFieldPos("f1"),"v14");
  tuple.setField(schema1.getFieldPos("f2"),"v24");
  tuple.setField(schema1.getFieldPos("f3"),34);
  tuple.setField(schema1.getFieldPos("f4"),"v44");
  tuple.setField(schema1.getFieldPos("f5"),54);
  tuple.printTuple();
  block->appendTuple(tuple);

  // the fifth tuple
  tuple.setField(schema1.getFieldPos("f1"),"v15");
  tuple.setField(schema1.getFieldPos("f2"),"v25");
  tuple.setField(schema1.getFieldPos("f3"),35);
  tuple.setField(schema1.getFieldPos("f4"),"v45");
  tuple.setField(schema1.getFieldPos("f5"),55);
  tuple.printTuple();
  block->appendTuple(tuple);
   

  cout << "Write memory block 0 into block 0 of the relation (on the disk):" << endl;
  relationPtr1->writeBlockFromMemory(0,0);
  relationPtr1->printRelation();
  cout << "The relation " << relationPtr1->getRelationName() << " has " << relationPtr1->getNumOfBlocks() << " blocks" << endl << endl;
    // Set up a block in the memory
  Tuple tuple2(&schema2);
  
  cout << "Insert the following tuples into memory block 0" << endl;
  block->clear(); //clear the block

  // Set up the first tuple
  tuple2.setField(schema2.getFieldPos("f1"),"v11");
  tuple2.setField(schema2.getFieldPos("f2"),"v21");
  tuple2.setField(schema2.getFieldPos("f3"),31);
  tuple2.setField(schema2.getFieldPos("f4"),"v41");
  tuple2.setField(schema2.getFieldPos("f5"),51);
  tuple2.printTuple();
  block->appendTuple(tuple2);

  // the second tuple
  tuple2.setField(schema2.getFieldPos("f1"),"v12");
  tuple2.setField(schema2.getFieldPos("f2"),"v22");
  tuple2.setField(schema2.getFieldPos("f3"),32);
  tuple2.setField(schema2.getFieldPos("f4"),"v42");
  tuple2.setField(schema2.getFieldPos("f5"),52);
  tuple2.printTuple();
  block->appendTuple(tuple2);

  // the third tuple
  tuple2.setField(schema2.getFieldPos("f1"),"v13");
  tuple2.setField(schema2.getFieldPos("f2"),"v23");
  tuple2.setField(schema2.getFieldPos("f3"),33);
  tuple2.setField(schema2.getFieldPos("f4"),"v43");
  tuple2.setField(schema2.getFieldPos("f5"),53);
  tuple2.printTuple();
  block->appendTuple(tuple2);

  // the four tuple
  tuple2.setField(schema2.getFieldPos("f1"),"v14");
  tuple2.setField(schema2.getFieldPos("f2"),"v24");
  tuple2.setField(schema2.getFieldPos("f3"),34);
  tuple2.setField(schema2.getFieldPos("f4"),"v44");
  tuple2.setField(schema2.getFieldPos("f5"),54);
  tuple2.printTuple();
  block->appendTuple(tuple2);

  // the fifth tuple
  tuple2.setField(schema2.getFieldPos("f1"),"v15");
  tuple2.setField(schema2.getFieldPos("f2"),"v25");
  tuple2.setField(schema2.getFieldPos("f3"),35);
  tuple2.setField(schema2.getFieldPos("f4"),"v45");
  tuple2.setField(schema2.getFieldPos("f5"),55);
  tuple2.printTuple();
  block->appendTuple(tuple2);

   cout << "Write memory block 0 into block 0 of the relation (on the disk):" << endl;
  relationPtr2->writeBlockFromMemory(0,0);
  relationPtr2->printRelation();
  cout << "The relation " << relationPtr2->getRelationName() << " has " << relationPtr2->getNumOfBlocks() << " blocks" << endl << endl;

}