#pragma once

#ifndef MYDBTABLE
#define MYDBTABLE
#include "RawTable.h"
#include <vector>
#include <map>

class Table : public RawTable
{
public:
	Table(BufferManager &_bm, unsigned _metaAddr);
	Table(BufferManager &_bm, int _tupleSize, unsigned _searchKey, std::vector<unsigned> _cols);
	~Table();
	int insert(std::map<unsigned, unsigned char*>, bool distinct);
	int remove(std::map<unsigned, unsigned char*>);
	std::vector<unsigned char*> select(std::map<unsigned, unsigned char*>);

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

};


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
	int insert(std::map<unsigned, unsigned char*>, bool distinct);
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
	/*
	功能：
	给定磁盘块地址范围内的线性搜索
	参数：
	cond：条件字节指针
	beg：条件在元组中的起始位置
	end：条件在元组中的终止位置
	from：起始搜索地址，默认-1，从第一个磁盘块开始搜索
	steps：搜索的磁盘块数
	返回：
	终止的比较结果、磁盘块地址、块内偏移
	*/
	std::pair<int, std::pair<unsigned, unsigned>>
		linearSerach(unsigned char* cond, unsigned beg, unsigned len, unsigned from, unsigned steps);


private:
	//helper cache
	unsigned char *cache = new unsigned char[bm.getBlkSize()];
};

#endif 
