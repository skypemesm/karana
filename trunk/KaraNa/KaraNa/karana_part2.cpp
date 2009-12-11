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

//Function prototypes
int logical2physical();
int printresults();
void parse (string );
void cost_calculation();
void order_join ();
void run_operations();
vector <Tuple> sort_by (string , vector<string> );
vector<Tuple> twopassDupElim(string );
void ksort(vector <Tuple> &array, vector <int >, vector<string>);
void quickSort(vector <Tuple> &arr, int , int , vector <int >, vector<string>);
int tuple_comp(Tuple , Tuple ,  vector <int >, vector<string>);
bool areTuplesEqual(Tuple , Tuple);
int GetFirstFreeMemBlock();
bool GetTupleVal(Tuple& t, node* condition, string relation);
vector<string> checksubtree(node* root);
string onepass_selection(string Relation, node* condition);
string onepass_projection(string relation, Projection* p, bool ifPrint);
string RemoveDuplicates(string relation);
bool JoinRelations(Tuple& tA, string relA, Tuple& tB, string relB, node* condition, SchemaManager sm);
bool Join_onepass(string tableA,string tableB,node* condition,vector<vector<Tuple>*>& _tuplepair);
//void CreateDebugData();
void ExecuteQuery();

 struct comp
{
	bool operator()(const Tuple A, const Tuple B)
	{
		return memcmp(&A,&B,sizeof(Tuple))<0;
	}
};

/**
* This function converts the logical tree to a physical plan.
*/
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

/**
* This method prints the results for a query.
*/
int printresults()
{
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
	return 0;
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
vector<Tuple> twopassDupElim(string relationname)
{
	vector<Tuple> results, tuples_in_memory;
	Relation* thisrelation = schemaMgr.getRelation (relationname);
	Schema* thisschema = schemaMgr.getSchema (relationname);
	
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
	string newRel=relation.append("_seltemp"); 
	Relation* newrel=schemaMgr.createRelation(newRel,*schemaMgr.getSchema(relation));
	int free_mem_blocks=0;
	int first_free_mem_index=GetFirstFreeMemBlock();
	if(first_free_mem_index==-1)
		return "null";
	int second_free_mem_index=GetFirstFreeMemBlock();
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
				for(vector<Tuple>::iterator it=mem.getBlock(first_free_mem_index)->getTuples().begin();it!=mem.getBlock(first_free_mem_index)->getTuples().end();it++)
				{
					if(GetTupleVal(*it,condition,relation)==true)
					{
						if(mem.getBlock(second_free_mem_index)->isFull())
						{
							newrel->writeBlockFromMemory(newrel->getNumOfBlocks(),second_free_mem_index);
							mem.getBlock(second_free_mem_index)->clear();
						}

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
	string newRel=relation.append("_proj");
 
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
	int second_free_mem_index=GetFirstFreeMemBlock();
	if(second_free_mem_index==-1)
		return "null";
	
	for(int i=0;i<rel->getNumOfBlocks();i++)
		{
			if(schemaMgr.getRelation(relation)->readBlockToMemory(i,first_free_mem_index)==true)
			{
				for(vector<Tuple>::iterator it=mem.getBlock(first_free_mem_index)->getTuples().begin();it!=mem.getBlock(first_free_mem_index)->getTuples().end();it++)
				{
					Tuple t(&newrelscm);
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
set<Tuple,comp>duplicates;
Relation* rel=schemaMgr.getRelation(relation);
string newRel=relation.append("_dupremoved"); 
Relation* newrel=schemaMgr.createRelation(newRel,*schemaMgr.getSchema(relation));
int free_mem_blocks=0;
int first_free_mem_index=GetFirstFreeMemBlock();
if(first_free_mem_index==-1)
return "null";
int second_free_mem_index=GetFirstFreeMemBlock();
if(second_free_mem_index==-1)
return "null";
for(int i=0;i<rel->getNumOfBlocks();i++)
		{
			if(schemaMgr.getRelation(relation)->readBlockToMemory(i,first_free_mem_index)==true)
			{
				for(vector<Tuple>::iterator it=mem.getBlock(first_free_mem_index)->getTuples().begin();it!=mem.getBlock(first_free_mem_index)->getTuples().end();it++)
				{
					duplicates.insert(*it);
				}
			}

			mem.getBlock(first_free_mem_index)->clear();

		}
vector<Tuple>distinct;

for(set<Tuple,comp>::iterator it=duplicates.begin();it!=duplicates.end();it++)
{
	distinct.push_back(*it);
}
second_free_mem_index=GetFirstFreeMemBlock();
int reqsize;
reqsize=distinct.size()%schemaMgr.getSchema(relation)->getTuplesPerBlock()==0?distinct.size():distinct.size()+1;
for(int j=0;j<reqsize;j++)
{
	if(mem.getBlock(second_free_mem_index+j)->isFull()==true)
		return "null";
}
mem.setTuples(second_free_mem_index,distinct);
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
	first_free_mem_index=GetFirstFreeMemBlock();
	for(int j=0;j<bigrel->getNumOfBlocks();j++)
	{
		if(bigrel->readBlockToMemory(j,first_free_mem_index)==true)
			{
				for(vector<Tuple>::iterator it=mem.getBlock(first_free_mem_index)->getTuples().begin();it!=mem.getBlock(first_free_mem_index)->getTuples().end();it++)
				{
					for(int k=0;k<smallrel->getNumOfBlocks();k++)
					{
						for(vector<Tuple>::iterator sit=mem.getBlock(s_index+k)->getTuples().begin();sit!=mem.getBlock(s_index+k)->getTuples().end();sit++)
						{
							if(JoinRelations(*sit,smallrel->getRelationName(),*it,bigrel->getRelationName(),condition,schemaMgr))
							{
								vector<Tuple>*temp=new vector<Tuple>();
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

void ExecuteQuery()
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
		}
		else
		{

		}
		//2 table join
	}
	else
	{
		;//no join
	}
}