#pragma once

#ifndef MYDBTABLE
#define MYDBTABLE
#include "RawTable.h"
#include <vector>
#include <map>

/*
顺序表
接口：在底层表的基础上进一步封装，提供了属性层次的顺序（按照搜索码排列）表单CRUD
*/
class SeqTable : public RawTable
{
public:
	SeqTable(BufferManager &_bm, unsigned _metaAddr);
	SeqTable(BufferManager &_bm, int _tupleSize, unsigned _searchKey, std::vector<unsigned> _cols, bool _cmp);
	~SeqTable();
	int insert(std::map<unsigned, unsigned char*>);
	int remove(std::map<unsigned, unsigned char*>);
	std::vector<unsigned char*> select(std::map<unsigned, unsigned char*>);

	//for testing
	std::vector<unsigned> binarySearch(unsigned char* cond);

protected:
	//meta info
	unsigned searchKey;
	std::vector<unsigned> cols;

	//important data structure
	//map from logical linked list addr to actual disc number
	std::vector<unsigned> index;

	//helper functions
	void format(unsigned char* blk, std::map<unsigned, unsigned char*> args);

private:
	//helper cache
	unsigned char *cache = new unsigned char[bm.getBlkSize()];
};

#endif 
