#pragma once
#ifndef MYDB
#define MYDB
#include "Table.h"
#include "Index.h"
#include "BufferManager.h"
#include <vector>
#include <string>
#include <unordered_map>
#include <stdexcept>

/*
数据库
Table和Index的集合以及对其的操作
*/

class DB
{
public:
	DB(BufferManager& _bm):bm(_bm){}
	~DB();
	//基本创建、写磁盘、销毁操作
	Table& createSeqTable(std::string tableName, unsigned _searchKey, std::vector<unsigned> _cols, bool _cmp);
	Index& createHashIndexOn(std::string indexName, std::string tableName, unsigned attr, unsigned _bucketSize);
	/*
	void saveTable(std::string tableName);
	void saveIndex(std::string indexName);
	void saveDB();
	void dropTable(std::string tableName);
	void dropIndex(std::string indexName);
	void dropDB();
	*/
	//添加元组
	int insertInto(std::string tableName, std::map<unsigned, unsigned char*> cond, bool distinct);
	//删除元组
	int removeFrom(std::string tableName, std::map<unsigned, unsigned char*> cond);
	//关系选择
	std::vector<unsigned char*> selectFrom(std::string tableName, std::map<unsigned, unsigned char*> cond); 
	//关系连接
	/*
	std::vector<unsigned char*> selectJoin
	(std::pair<std::string, std::string>tableNames, std::vector<std::pair<unsigned, unsigned>> cond);
	//投影
	std::vector<unsigned char*> projectOf(std::vector<unsigned char*>& rawResult, std::vector<std::pair<unsigned, unsigned>> cond);
	//集合求交
	std::vector<unsigned char*> unionOf(std::pair<std::vector<unsigned char*>&, std::vector<unsigned char*>&> tables);
	//集合求并
	std::vector<unsigned char*> intersectOf(std::pair<std::vector<unsigned char*>&, std::vector<unsigned char*>&> tables);
	//集合求差
	std::vector<unsigned char*> diffOf(std::pair<std::vector<unsigned char*>&, std::vector<unsigned char*>&> tables);
	*/
private:
	std::unordered_map<std::string, SeqTable*> tables;
	std::unordered_map<std::string, HashIndex*> indexes;
	//map from table_name to (attr, index_name)
	std::unordered_map<std::string, std::vector<std::pair<unsigned, std::string>>> tabIdx;
	BufferManager &bm;

	//helper functions
	int doRemove(std::string tableName, unsigned char* &blk, std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned addr);
};

#endif // !MYDB
