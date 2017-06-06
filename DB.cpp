#include "DB.h"
#include <algorithm>
#include <iostream>

Table & DB::createSeqTable(std::string tableName, unsigned _searchKey, std::vector<unsigned> _cols, bool _cmp)
{
	tables.emplace(tableName, 
		new SeqTable(bm, _cols.back(), _searchKey, _cols, _cmp));
	return *tables[tableName];
}

Index & DB::createHashIndexOn(std::string indexName, std::string tableName, unsigned attr, unsigned _bucketSize)
{
	auto& table = *tables[tableName];
	indexes.emplace(indexName, 
		new HashIndex(bm, table.cols[attr] - table.cols[attr-1], _bucketSize));
	//初始化索引
	auto& index = *indexes[indexName];
	unsigned beg = table.cols[attr - 1];
	for (auto addr : table.index)
	{
		unsigned char* blk = bm.read(addr);
		for (unsigned char* p = blk + 8; p != *(unsigned*)blk + blk; p += table.getTupleSize())
		{
			index.insert(p + beg, addr, p - blk);
		}
	}
	tabIdx[tableName].emplace_back(attr, indexName);
	return index;
}

int DB::insertInto(std::string tableName, std::map<unsigned, unsigned char*> cond, bool distinct)
{
	auto& table = *tables[tableName];
	auto insertResult = table.insert(cond, distinct);
	if (insertResult.first == 0)
	{
		return 0;
	}
	unsigned blk = insertResult.second.first;
	unsigned offset = insertResult.second.second;
	//更新索引
	for (auto p : tabIdx[tableName])
	{
		//找到一个索引
		unsigned attr = p.first;
		if (cond.find(attr) != cond.end())
		{
			indexes[p.second]->insert(cond[attr], blk, offset);
		}
	}
	return 1;
}

int DB::removeFrom(std::string tableName, std::map<unsigned, unsigned char*> cond)
{
	return tables[tableName]->remove(cond);
}

std::vector<unsigned char*> DB::selectFrom(std::string tableName, std::map<unsigned, unsigned char*> cond)
{
	return tables[tableName]->select(cond);
}
