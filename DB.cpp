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
	//��ʼ������
	auto& index = *indexes[indexName];
	unsigned beg = table.cols[attr - 1];
	for (auto addr : table.index)
	{
		unsigned char* blk = bm.read(addr);
		for (unsigned char* p = blk + 8; p != *(unsigned*)blk + blk; p += table.getTupleSize())
		{
			index.insert(p + beg, addr);
		}
	}
	tabIdx[tableName].emplace_back(attr, indexName);
	return index;
}

int DB::insertInto(std::string tableName, std::map<unsigned, unsigned char*> cond, bool distinct)
{
	auto& table = *tables[tableName];
	auto insertResult = table.insert(cond, distinct);
	unsigned newBlkIndex = insertResult.first;
	unsigned newTupleIndex = insertResult.second;
	//����ʧ��
	if (newBlkIndex == 0)
	{
		return 0;
	}
	//����ɹ�������ÿһ������
	for (auto p : tabIdx[tableName])
	{
		//�ҵ�һ������
		unsigned attr = p.first;
		auto& index = indexes[p.second];
		//������Ԫ���Ӧ������
		if (cond.find(attr) != cond.end())
		{
			index->insert(cond[attr], table.index[newTupleIndex]);
		}
		//����λ�ñ仯��Ԫ�������
		if (newBlkIndex != -1)
		{
			unsigned newBlkAddr = table.index[newBlkIndex];
			unsigned prevAddr = table.index[newBlkIndex - 1];
			unsigned char* blk = bm.read(newBlkAddr);
			bool flag = (newBlkIndex == newTupleIndex);
			for (unsigned char* p = blk + 8; p != *(unsigned*)blk + blk; p += table.getTupleSize())
			{
				if (flag &&
					memcmp(p + table.cols[attr - 1], cond[attr], table.cols[attr] - table.cols[attr - 1]) == 0)
				{
					flag = false;
					continue;
				}
				//ɾ��ԭ����������
				index->remove(p + table.cols[attr - 1], prevAddr);
				//�����µ�������
				index->insert(p + table.cols[attr - 1], newBlkAddr);
			}
		}
	}
	return 1;
}

int DB::removeFrom(std::string tableName, std::map<unsigned, unsigned char*> cond)
{
	auto& table = *tables[tableName];

	return tables[tableName]->remove(cond);
}

std::vector<unsigned char*> DB::selectFrom(std::string tableName, std::map<unsigned, unsigned char*> cond)
{
	return tables[tableName]->select(cond);
}
