#ifndef _STORAGE_MANAGER_H
#define _STORAGE_MANAGER_H

#include<vector>
#include<map>

using namespace std;

#define FIELDS_PER_BLOCK 32 // i.e. At least 4 tuples per block
#define MAX_NUM_OF_FIELDS_IN_RELATION 8
#define NUM_OF_BLOCKS_IN_MEMORY 100

void setDelay(int delay);
void delay();

//A schema records field names and their types
class Schema{
  private:
  // Maps a field name(string) to a field type(string) and a position(int).
  // A position refers to the vector index of the field in a Tuple object).
  // A field type can either be "INT" or "STR20"
  map<string,pair<string,int> > fields;
  int num_of_strings;
  int num_of_integers;
  int tuples_per_block;
  void clear();

  public:
  friend class Tuple;
  friend class SchemaManager;

  Schema();
  Schema(const vector<string>& field_names, const vector<string>& field_types);
  void printSchema() const;
  string getFieldType(string field_name) const; //return empty string if field not found
  int getFieldPos(string field_name) const; //return -1 if field not found
  int getNumOfFields() const;
  int getNumOfInt() const;
  int getNumOfString() const;
  int getTuplesPerBlock() const; //All blocks used by a relation with this schema must hold no more than this number of tuples/records
};

//A tuple equals a record/row in a relation/table
class Tuple{
  private:
  Schema* schema; // the schema of the relation which the tuple belongs to
  vector<int> i;  // stores integers
  vector<string> s; // stores strings

  public:
  friend class Block;
  friend class MainMemory;
  friend class Relation;

  Tuple(Schema* schema);
  bool setField(int pos,int val); // returns false if out of bound
  bool setField(int pos,string val); // returns false if out of bound
  int getInt(int pos) const; // returns 0 if out of bound
  string getString(int pos) const; // returns empty string if out of bound
  void printTuple() const; // prints the field values
};

// A block holds several tuples/records (limited by FIELDS_PER_BLOCK)
// Assumes all tuples in a block belong to the same relation/table
class Block {
  private:
  vector<Tuple> tuples;
  Block(); // private constructor: you can only use the blocks located in the MainMemory and Relation (on the disk)

  public:
  friend class MainMemory;
  friend class Relation;

  bool isFull() const;
  void clear(); //empty the block
  int getNumTuples() const; // returns current number of tuples inside this block
  Tuple getTuple(int tuple_offset) const; // gets the tuple value at tuple_index; returns empty Tuple if tuple_index out of bound
  vector<Tuple> getTuples() const; // returns all the tuples inside this block
  bool setTuple(int tuple_offset, const Tuple& tuple); // sets new tuple value at tuple_index; returns false if tuple_index out of bound
  bool setTuples(const vector<Tuple>& tuples); // sets new tuples for the block; returns false if number of input tuples exceeds the space limit
  bool appendTuple(const Tuple& tuple);  // appends one tuple to the end of the block; returns false if total number of tuples exceeds the space limit
  void printBlock() const;
};

// MainMemory holds NUM_OF_BLOCKS_IN_MEMORY many blocks
// Assumes no delay in accessing memory
class MainMemory {
  private:
  Block* blocks; // an array of blocks

  public:
  friend class Relation;

  MainMemory();
  Block* getBlock(int memory_block_index); //returns NULL if out of bound

  //Gets tuples from a range of blocks in memory[ memory_block_begin, memory_block_begin+num_blocks-1 ]
  //Assums that the output tuples all belong to the same relation/table.
  //However, the function does not check it for you. Make sure to check it by yourself.
  vector<Tuple> getTuples(int memory_block_begin,int num_blocks) const;

  //Writes tuples consecutively starting from memory block memory_block_begin; returns false if out of bound in memory
  //Assumes that the input tuples all belong to the same relation/table.
  //However, the function does not check it for you. Make sure to check it by yourself.
  bool setTuples(int memory_block_begin,const vector<Tuple>& tuples);

  int getMemorySize() const; // returns total number of blocks in the memory (including empty ones)
  void dumpMemory() const; // print memory content
};

// Assumes the data of each relation is clustered on the disk (stored consecutively in the disk blocks)
// Allows only reading and writing each block instead of reading each tuple from the relation
class Relation {
  private:
  Schema* schema; // the schema of the relation
  MainMemory* mem; // a pointer to the main memory
  vector<Block> data; // consecutive blocks; no limit on the nubmer of blocks
  string relation_name; // name of the relation
  Relation(); // private constructor: you can only create a relation through schema manager but no through here
  void setSchema(Schema* schema);
  void setName(string relation_name);
  void setMemory(MainMemory* mem);
  void clear();

  public:
  friend class SchemaManager;
  friend class RelationSchemaPair;

  string getRelationName() const;
  int getNumOfBlocks() const;
  int getNumOfTuples() const; //assumes every block used by the relation except for the last one is full

  //Every call to each of the following function has a simulated disk delay

  //reads one block from the relation (the disk) and stores in the memory
  bool readBlockToMemory(int relation_block_index, int memory_block_index) const; // returns false if the index is out of bound

  //reads one block from the memory and stores in the relation (on the disk)
  bool writeBlockFromMemory(int relation_block_index, int memory_block_index); // returns false if the index is out of bound

  //delete the block from [starting_block_index] to the last block
  bool deleteBlock(int starting_block_index); // return false if out of bound

  void printRelation() const; // print all tuples
};

// The schema manager creates a relation according to the input schema
// and stores schema-relation pairs

// A data structure that links a relation to a schema
struct RelationSchemaPair{
  Relation relation;
  Schema schema;
};
class SchemaManager{
  private:
  MainMemory* mem;
  map<string,int> relationName_to_pair_position;
  vector<struct RelationSchemaPair> relation_schema_pairs;

  public:
  SchemaManager(MainMemory* mem);
  Relation* createRelation(string relationName,const Schema& schema); //returns a pointer to the newly allocated relation according to the schema
  //Schema getSchema(string relationName); //obsolete
  Schema* getSchema(string relationName); //returns empty schema if the relation is not found
  Relation* getRelation(string relationName); //returns NULL if the relation is not found
  bool deleteRelation(string relationName); //returns false if the relation is not found
  void printRelationSchemaPairs() const; //print all relations and their schema
};

#endif
