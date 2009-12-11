#include<iostream>
#include<ctime>
#include<string>
#include "StorageManager.h"

using namespace std;

int delay_time=20;
unsigned long int DIOs=0;

void setDelay(int delay){
  delay_time=delay;
}

void delay(){
  clock_t start_time;
  start_time=clock();
  while(clock()-start_time<delay_time*CLOCKS_PER_SEC/1000){
   ;
  }
  //cerr<<"delay_time= "<<delay_time<<endl;
  //cerr<<"start_time= "<<start_time<<" clocks_per_sec= "<<CLOCKS_PER_SEC<<endl;
}

// Reset the disk I/O counter.
// Every time you receive a query, reset the counter before you execute the query.
void resetDIOs() {
  DIOs=0;
}

// After the query is done, you could get accumulated number of disk I/Os using this function.
// One disk I/O means one access to a disk block (i.e. a relation block).
unsigned long int getDIOs() {
  return DIOs;
}

Schema::Schema() {}

Schema::Schema(const vector<string>& field_names, const vector<string>& field_types){
  if(field_names.size()!=field_types.size()){
    cerr<<"Schema ERROR: number of field field_names and number of field field_types do not match"<<endl;
    return;
  }
  num_of_integers=0;
  num_of_strings=0;
  for(int i=0;i<field_names.size();i++){
     if(field_types[i]=="INT"){
       fields[field_names[i]]=pair<string,int>(field_types[i],num_of_integers);
       num_of_integers++;
       if(num_of_integers+num_of_strings>MAX_NUM_OF_FIELDS_IN_RELATION){
         cerr<<"Schema ERROR: at most "<<MAX_NUM_OF_FIELDS_IN_RELATION<<" fields are allowed"<<endl;
         return;
       }
     }else if(field_types[i]=="STR20"){
       fields[field_names[i]]=pair<string,int>(field_types[i],num_of_strings);
       num_of_strings++;
       if(num_of_integers+num_of_strings>MAX_NUM_OF_FIELDS_IN_RELATION){
         cerr<<"Schema ERROR: at most "<<MAX_NUM_OF_FIELDS_IN_RELATION<<" fields are allowed"<<endl;
         return;
       }
     }else{
       cerr<<"Schema ERROR: "<<field_types[i]<<" is not supported"<<endl;
     }
  }
  tuples_per_block=FIELDS_PER_BLOCK/(num_of_integers+num_of_strings);
}

void Schema::printSchema() const{
  map<string,pair<string,int> >::const_iterator it;
  it=fields.begin();
  cout<<"name\t type\t position"<<endl;
  while(it!=fields.end()) {
    cout<<it->first<<"\t"<<it->second.first<<"\t"<<it->second.second<<endl;
    it++;
  }
}

vector <string> Schema::getAllColumnNames() const{
	vector <string> columnNames;
  map<string,pair<string,int> >::const_iterator it;
  it=fields.begin();
  while(it!=fields.end()) {
	  columnNames.push_back(it->first);
    it++;
  }
  return columnNames;
}

string Schema::getFieldType(string field_name) const{
  map<string,pair<string,int> >::const_iterator mit;
  if ((mit=fields.find(field_name))==fields.end()) {
    cerr<<"getFieldPos ERROR: field name "<<field_name<<" is not found"<<endl;
    return string();
  }
  return mit->second.first;
}

int Schema::getFieldPos(string field_name) const {
  map<string,pair<string,int> >::const_iterator mit;
  if ((mit=fields.find(field_name))==fields.end()) {
    cerr<<"getFieldPos ERROR: field name "<<field_name<<" is not found"<<endl;
    return -1;
  }
  return mit->second.second;
}
int Schema::getNumOfFields() const {
  return fields.size();
}
int Schema::getNumOfInt() const{
  return num_of_integers;
}
int Schema::getNumOfString() const{
  return num_of_strings;
}
int Schema::getTuplesPerBlock() const{
  //return FIELDS_PER_BLOCK/(num_of_integers+num_of_strings);
  return tuples_per_block;
}

Tuple::Tuple(Schema* schema){
  i.resize(schema->getNumOfInt());
  s.resize(schema->getNumOfString());
  this->schema=schema;
}
int Tuple::getNumOfInts() {
	return i.size();
}

int Tuple::getNumOfStrings() {
	return s.size();
}

bool Tuple::isNull() { //returns true if the tuple is invalid
  return schema==NULL;
}

bool Tuple::setField(int pos,int val){
  if(pos<i.size() && pos>=0){
    i[pos]=val;
  } else{
    cerr<<"setField ERROR: pos "<<pos<<" is out of bound!"<<endl;
    return false;
  }
  return true;
}

bool Tuple::setField(int pos,string val){
  //cerr<<"in setField pos="<<pos<<" val="<<val<<endl;
  if(pos<s.size() && pos>=0){
    s[pos]=val;
  }else{
    cerr<<"setField ERROR: pos "<<pos<<" is out of bound!"<<endl;
    return false;
  }
  return true;
}

int Tuple::getInt(int pos) const{
  if(pos<i.size() && pos>=0){
    return i[pos];
  } else {
    cerr<<"getInt ERROR: pos "<<pos<<" is out of bound!"<<endl;
    return 0;
  }
}

string Tuple::getString(int pos) const{
  if(pos<s.size() && pos>=0){
    return s[pos];
  }else{
    cerr<<"getString ERROR: pos "<<pos<<" is out of bound!"<<endl;
    return string();
  }
}

void Tuple::printTuple() const{
  map<string,pair<string,int> >::iterator it;
/*
  it=schema->fields.begin();
  while(it!=schema->fields.end()){
    cout << it->first << "\t";
    it++;
  }
  cout << endl;
*/
  it=schema->fields.begin();
  while(it!=schema->fields.end()){
    if (schema->getFieldType(it->first)=="INT") {
      cout << i[schema->getFieldPos(it->first)] << "\t";
    } else if (schema->getFieldType(it->first)=="STR20") {
      cout << s[schema->getFieldPos(it->first)] << "\t";
    }
    it++;
  }
  cout<<endl;
}

Block::Block() {}

bool Block::isFull() const {
  if (tuples.empty()) return false;
  if (tuples.size()==tuples.front().schema->getTuplesPerBlock()) return true;
  return false;
}

void Block::clear() {
  tuples.clear();
}

int Block::getNumTuples() const {
  return tuples.size();
}

Tuple Block::getTuple(int tuple_offset) const { // gets the tuple value at tuple_index; returns empty Tuple if tuple_index out of bound
  if (!tuples.empty() && tuple_offset>=tuples.front().schema->getTuplesPerBlock()) {
    cerr << "getTuple ERROR: tuple offet out of bound of the block" << endl;
    return Tuple(NULL);
  }
  if (tuple_offset>=tuples.size()) {
    cerr << "getTuple ERROR: tuple offet out of bound" << endl;
    return Tuple(NULL);
  }
  return tuples[tuple_offset];
}

vector<Tuple> Block::getTuples() const {
  return tuples;
}

bool Block::setTuple(int tuple_offset, const Tuple& tuple) { // sets new tuple value at tuple_index; returns false if tuple_index out of bound
  if (!tuples.empty() && tuple_offset>=tuples.front().schema->getTuplesPerBlock()) {
    cerr << "getTuple ERROR: tuple offet out of bound of the block" << endl;
    return false;
  }
  if (tuple_offset>=tuples.size()) {
    cerr << "getTuple ERROR: tuple offet out of bound" << endl;
    return false;
  }
  tuples[tuple_offset]=tuple;
  return true;
}

bool Block::setTuples(const vector<Tuple>& tuples) {
  if (tuples.size()>tuples.front().schema->getTuplesPerBlock()) {
    cerr << "setTuples ERROR: number of tuples exceed space limit of the block" << endl;
    return false;
  }
  this->tuples.assign(tuples.begin(),tuples.end());
  return true;
}

bool Block::appendTuple(const Tuple& tuple) {
  if (isFull()) {
    cerr << "appendTuple ERROR: the block is full" << endl;
    return false;
  }
  this->tuples.push_back(tuple);
  return true;
}

void Block::printBlock() const {
  if (tuples.empty()) cout << endl;
  for (vector<Tuple>::const_iterator lit=tuples.begin();lit!=tuples.end();lit++) {
    lit->printTuple();
  }
}

Relation::Relation() {schema=NULL; mem=NULL;}

void Relation::setSchema(Schema* schema) {
  this->schema=schema;
}

void Relation::setName(string relation_name) {
  this->relation_name=relation_name;
}

void Relation::setMemory(MainMemory* mem) {
  this->mem=mem;
}

string Relation::getRelationName() const {
  return relation_name;
}

int Relation::getNumOfBlocks() const {
  return data.size();
}

// returns actual number of tuples in the relation
int Relation::getNumOfTuples() const {
  /*
  //assumes every block except for the last one used by the relation is full
  return (data.size()-1)*schema->getTuplesPerBlock() + data[data.size()-1].getNumTuples();
  */
  int total_tuples=0;
  for (vector<Block>::const_iterator vit=data.begin();vit!=data.end();vit++) {
    total_tuples+=vit->getNumTuples();
  }
  return total_tuples;
}

bool Relation::readBlockToMemory(int relation_block_index, int memory_block_index) const {
  delay();
  DIOs++;
  if (memory_block_index>=NUM_OF_BLOCKS_IN_MEMORY) {
    cerr << "readBlockToMemory ERROR: block index out of bound in memory" << endl;
    return false;
  }
  if (relation_block_index>=data.size()) {
    cerr << "readBlockToMemory ERROR: block index out of bound in relation" << endl;
    return false;
  }
  mem->blocks[memory_block_index]=data[relation_block_index];
  return true;
}

bool Relation::writeBlockFromMemory(int relation_block_index, int memory_block_index) {
  delay();
  DIOs++;
  if (memory_block_index>=NUM_OF_BLOCKS_IN_MEMORY) {
    cerr << "writeBlockFromMemory ERROR: block index out of bound in memory" << endl;
    return false;
  }

/*
  if (relation_block_index>data.size()) {
    cerr << "writeBlockFromMemory ERROR: block index out of bound in relation" << endl;
    return false;
  } else if (relation_block_index==data.size()) { // append a block
    if (data.size()>0 && (data[data.size()-1].getNumTuples()==0 || data[data.size()-1].getNumTuples() < data[data.size()-1].getTuple(0).schema->getTuplesPerBlock())) {
      cerr << "writeBlockFromMemory ERROR: append a block while the last block in the relation is not full" << endl;
      return false;
    }
    data.push_back(mem->blocks[memory_block_index]);
  } else   { // rewrite a block
    if (mem->blocks[memory_block_index].getNumTuples()==0 || mem->blocks[memory_block_index].getNumTuples() < mem->blocks[memory_block_index].getTuple(0).schema->getTuplesPerBlock()) {
      cerr << "writeBlockFromMemory ERROR: rewrite a disk block with insufficient number of input tuples" << endl;
      return false;
    }
    data[relation_block_index]=mem->blocks[memory_block_index];
  } */

  // block index out of bound; add more disk blocks
  int j=data.size();
  for (int i=j;i<=relation_block_index;i++)
    data.push_back(Block());
  data[relation_block_index]=mem->blocks[memory_block_index];

  return true;
}

//delete the block from [starting_block_index] to the last block
bool Relation::deleteBlock(int starting_block_index) {
  if (starting_block_index >= data.size()) {
    cerr << "deleteBlock ERROR: block index out of bound in relation" << endl;
    return false;
  }
  data.erase(data.begin()+starting_block_index,data.end());
  return true;
}

void Relation::printRelation() const {
  int i=0;
  cout << "******RELATION DUMP BEGIN******" << endl;
  for (vector<Block>::const_iterator vit=data.begin();vit!=data.end();vit++) {
    cout << i << ":";
    vit->printBlock();
    i++;
  }
  cout << "******RELATION DUMP END******" << endl;
}

MainMemory::MainMemory() { blocks=new Block[NUM_OF_BLOCKS_IN_MEMORY]; }

Block* MainMemory::getBlock(int memory_block_index) {
  if (memory_block_index>=NUM_OF_BLOCKS_IN_MEMORY) {
    cerr << "getBlock ERROR: block index out of bound in memory" << endl;
    return NULL;
  }
  return blocks+memory_block_index;
}

int MainMemory::getMemorySize() const { //returns max number of blocks
  return NUM_OF_BLOCKS_IN_MEMORY;
}

vector<Tuple> MainMemory::getTuples(int memory_block_begin,int num_blocks) const { //gets tuples from a range of blocks
  vector<Tuple> tuples;
  for (int i=memory_block_begin;i<memory_block_begin+num_blocks;i++) {
    vector<Tuple> tuples2=blocks[i].getTuples();
    tuples.insert(tuples.end(),tuples2.begin(),tuples2.end());
  }
  return tuples;
}

bool MainMemory::setTuples(int memory_block_begin,const vector<Tuple>& tuples) { //writes tuples consecutively starting from a particular memory block; returns false if out of bound in memory
  int tuples_per_block=tuples.front().schema->getTuplesPerBlock();
  int num_blocks=tuples.size()/tuples_per_block;
  int num_additional_blocks=(tuples.size()%tuples_per_block>0?1:0);
  if (memory_block_begin + num_blocks + num_additional_blocks >
       NUM_OF_BLOCKS_IN_MEMORY) {
    cerr << "setTuples ERROR: number of tuples exceed the memory space" << endl;
    return false;
  }
  vector<Tuple>::const_iterator lit=tuples.begin(),lit2=tuples.begin();
  int i,j;
  for (i=memory_block_begin;i<memory_block_begin + num_blocks;i++) {
    for (j=0;j<tuples_per_block;j++,lit2++);
    blocks[i].tuples.assign(lit,lit2);
    lit=lit2;
  }
  if (num_additional_blocks==1) {
    blocks[i].tuples.assign(lit,tuples.end());
  }
  return true;
}

void MainMemory::dumpMemory() const {
  cout << "******MEMORY DUMP BEGIN******" << endl;
  for (int i=0;i<NUM_OF_BLOCKS_IN_MEMORY;i++) {
    cout << i << ": ";
    blocks[i].printBlock();
  }
  cout << "******MEMORY DUMP END******" << endl;
}

SchemaManager::SchemaManager(MainMemory* mem) {
  this->mem=mem;
}

Relation* SchemaManager::createRelation(string relationName,const Schema& schema){
/*
  int pos=relation_schema_pairs.size();
  relationName_to_pair_position[relationName]=pos;

  //relation_schema_pairs.resize(pos+1);
  struct RelationSchemaPair rs;
  relation_schema_pairs.push_back(rs);
  relation_schema_pairs[pos].schema=schema;
  relation_schema_pairs[pos].relation.setName(relationName);
  relation_schema_pairs[pos].relation.setSchema(&(relation_schema_pairs[pos].schema));
  relation_schema_pairs[pos].relation.setMemory(mem);

  return &(relation_schema_pairs[pos].relation);
*/
  relationName_to_pair_position[relationName].schema=schema;
  relationName_to_pair_position[relationName].relation.setName(relationName);
  relationName_to_pair_position[relationName].relation.setSchema(&(relationName_to_pair_position[relationName].schema));
  //cerr << &(relationName_to_pair_position[relationName].schema) << " vs " << relationName_to_pair_position[relationName].relation.schema << endl;
  relationName_to_pair_position[relationName].relation.setMemory(mem);

  return &(relationName_to_pair_position[relationName].relation);
}

Schema* SchemaManager::getSchema(string relationName) {
  //map<string,int>::iterator it=relationName_to_pair_position.find(relationName);
  map<string,struct RelationSchemaPair>::iterator it=relationName_to_pair_position.find(relationName);
  if(it==relationName_to_pair_position.end()) {
    //cerr << "getSchema ERROR: relation " << relationName << " does not exist" << endl;
    return NULL;
  } else {
    //return &relation_schema_pairs[it->second].schema;
    return &(relationName_to_pair_position[relationName].schema);
  }
}

Relation* SchemaManager::getRelation(string relationName) {
  //map<string,int>::iterator it=relationName_to_pair_position.find(relationName);
  map<string,struct RelationSchemaPair>::iterator it=relationName_to_pair_position.find(relationName);
  if(it==relationName_to_pair_position.end()){
    //cerr << "getRelation ERROR: relation " << relationName << " does not exist" << endl;
    return NULL;
  } else {
    //return &(relation_schema_pairs[relationName_to_pair_position[relationName]].relation);
    return &(relationName_to_pair_position[relationName].relation);
  }
}

bool SchemaManager::deleteRelation(string relationName) {
  //map<string,int>::iterator it;
  map<string,struct RelationSchemaPair>::iterator it;
  if ((it=relationName_to_pair_position.find(relationName))==relationName_to_pair_position.end()) {
    cerr << "deleteRelation ERROR: relation " << relationName << " does not exist" << endl;
    return false;
  }
/*
  int pos=it->second;
  vector<struct RelationSchemaPair>::iterator vit;
  vit=relation_schema_pairs.begin();
  vit=vit+pos;
  relation_schema_pairs.erase(vit);*/
  relationName_to_pair_position.erase(it);
  return true;
}

void SchemaManager::printRelationSchemaPairs() const {
  map<string,struct RelationSchemaPair>::const_iterator it;
  for (it=relationName_to_pair_position.begin();it!=relationName_to_pair_position.end();it++) {
    cout << it->first << endl;
    //cerr << &(it->second.schema) << " vs " << it->second.relation.schema << endl;
    it->second.schema.printSchema();
  }
/*
  vector<struct RelationSchemaPair>::const_iterator vit;
  for (vit=relation_schema_pairs.begin();vit!=relation_schema_pairs.end();vit++) {
    cout << vit->relation.relation_name << endl;
    vit->schema.printSchema();
  }
*/
}

