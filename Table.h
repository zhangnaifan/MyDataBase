#pragma once

#ifndef MYDBTABLE
#define MYDBTABLE
#include "RawTable.h"
#include <vector>
#include <map>

class Table : public RawTable
{
	friend class DB;
public:
	Table(unsigned _metaAddr, BufferManager & _bm);
	Table(BufferManager &_bm, int _tupleSize, unsigned _searchKey, std::vector<unsigned> _cols);
	~Table();
	void save();
	void dropAll();

protected:
	//meta info
	unsigned searchKey;
	std::vector<unsigned> cols;

	//important data structure
	//map from logical linked list addr to actual disc number
	std::vector<unsigned> index;

	//helper functions
	void format(unsigned char* blk, std::map<unsigned, unsigned char*> args);

	//helper cache
	unsigned char *cache = new unsigned char[bm.getBlkSize()];
};


/*
顺序表
接口：在底层表的基础上进一步封装，提供了属性层次的顺序（按照搜索码排列）表单CRUD
*/
class SeqTable : public Table
{
	friend class DB;
public:
	SeqTable(unsigned _metaAddr, BufferManager & _bm);
	SeqTable(BufferManager &_bm, int _tupleSize, unsigned _searchKey, std::vector<unsigned> _cols, bool _cmp);
	std::pair<unsigned, unsigned> insert(std::map<unsigned, unsigned char*>, bool distinct);
	int remove(std::map<unsigned, unsigned char*>);
	std::vector<unsigned char*> select(std::map<unsigned, unsigned char*>);
	void save();

private:
	std::vector<unsigned> binarySearch(unsigned char* cond);
	std::pair<int, std::pair<unsigned, unsigned>>
		linearSerach(unsigned char* cond, unsigned beg, unsigned len, unsigned from, unsigned steps);
	bool cmp;
};

#endif 
