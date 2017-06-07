#include "DB.h"
#include <algorithm>
#include <iostream>
#include <vector>
#include <string>

DB::~DB()
{
	for (auto p : tables)
		delete p.second;
	for (auto p : indexes)
		delete p.second;
}

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
		for (unsigned char* p = blk + 8; p != *(unsigned*)blk + blk; p += table.tupleSize)
		{
			index.insert(p + beg, addr);
		}
	}
	tabIdx[tableName].emplace_back(attr, indexName);
	return index;
}

int DB::insertInto(std::string tableName, std::map<unsigned, unsigned char*> cond, bool distinct)
{
	/*
	ʹ��table��ɾ����
	��Ҫ������������Ҫ�������������
	һ���²����Ԫ����Ҫ��������
	����ԭ����Ԫ�飬������ڴ��̿��ַ�����仯����Ҫ��Ӧ������������
	*/
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
			for (unsigned char* p = blk + 8; p != *(unsigned*)blk + blk; p += table.tupleSize)
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

//�����ɾ�������д��ڶ�Ӧ��������
//�����������ҵ���ɾ��Ԫ�����ڵĴ��̿��ַ��
//����ֻ����дtable�е�removeʹ֧֮������������ɾ��
int DB::removeFrom(std::string tableName, std::map<unsigned, unsigned char*> cond)
{
	auto& table = *tables[tableName];
	std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> rmvArgs;
	for (auto p : cond)
	{
		rmvArgs.push_back({ p.second,{ table.cols[p.first - 1], table.cols[p.first] - table.cols[p.first - 1] } });
	}
	for (auto indexPair : tabIdx[tableName])
	{
		//�ҵ�һ������
		if (cond.find(indexPair.first) != cond.end())
		{
			unsigned attr = indexPair.first;
			auto& index = indexes[indexPair.second];
			auto addrs = index->get(cond[attr]);
			int res = 0;
			for (auto addr : addrs)
			{
				int cnt = table.rawRemove(rmvArgs, addr);
				for (int i = 0; i < cnt; ++i)
				{
					index->remove(cond[attr], addr);
				}
			}
			return res;
		}
	}
	auto& args = cond;
	auto& searchKey = table.searchKey;
	auto& index = table.index;
	auto& cols = table.cols;
	auto& numBlk = table.numBlk;
	auto& startAddr = table.startAddr;
	unsigned char* bufBlk;
	//���¿�����Table::remove()
	if (args.find(searchKey) != args.end())
	{
		auto pos = table.binarySearch(args[searchKey]);
		if (pos[0] != 0)
		{
			return 0;
		}
		std::vector<unsigned> candidate{ pos[1] };
		for (unsigned i = pos[1] - 1; i != -1; --i)
		{
			unsigned addr = index[i];
			auto linPos = table.linearSerach(args[searchKey], cols[searchKey - 1], cols[searchKey] - cols[searchKey - 1], addr, 1);
			if (linPos.first != 0)
				break;
			candidate.push_back(i);
		}
		for (unsigned i = pos[1] + 1; i < index.size(); ++i)
		{
			unsigned addr = index[i];
			auto linPos = table.linearSerach(args[searchKey], cols[searchKey - 1], cols[searchKey] - cols[searchKey - 1], addr, 1);
			if (linPos.first != 0)
				break;
			candidate.push_back(i);
		}
		int res = 0;
		for (auto addr : candidate)
		{
			res += doRemove(tableName, bufBlk, rmvArgs, index[addr]);
			//���̿�Ϊ�գ�����
			if (*(unsigned*)bufBlk == 8)
			{
				if (!addr)
				{
					if (numBlk == 1)
					{
						continue;
					}
					startAddr = *(unsigned*)(bufBlk + 4);
				}
				else
				{
					unsigned char* blk = bm.read(index[addr - 1]);
					*(unsigned*)(blk + 4) = *(unsigned*)(bufBlk + 4);
					bm.write(index[addr - 1]);
				}
				index.erase(index.begin() + addr);
				--numBlk;
			}
		}
		return res;
	}
	else
	{
		int res = 0;
		unsigned addr = 0;
		do {
			res += doRemove(tableName, bufBlk, rmvArgs, index[addr]);
			//���̿�Ϊ�գ�����
			if (*(unsigned*)bufBlk == 8)
			{
				if (!addr)
				{
					if (numBlk == 1)
					{
						continue;
					}
					startAddr = index[1];
				}
				else
				{
					unsigned char* blk = bm.read(index[addr - 1]);
					*(unsigned*)(blk + 4) = *(unsigned*)(bufBlk + 4);
					bm.write(index[addr - 1]);
				}
				index.erase(index.begin() + addr);
				--numBlk;
			}
			else
			{
				++addr;
			}
		} while (addr != index.size());
		return res;
	}
}

/*
����������������
������������select����
*/
std::vector<unsigned char*> DB::selectFrom(std::string tableName, std::map<unsigned, unsigned char*> cond)
{
	auto& table = *tables[tableName];
	std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> selArgs;
	for (auto p : cond)
	{
		selArgs.push_back({ p.second,{ table.cols[p.first - 1], table.cols[p.first] - table.cols[p.first - 1] } });
	}
	for (auto indexPair : tabIdx[tableName])
	{
		//�ҵ�һ������
		if (cond.find(indexPair.first) != cond.end())
		{
			unsigned attr = indexPair.first;
			auto& index = indexes[indexPair.second];
			auto addrs = index->get(cond[attr]);
			std::vector<unsigned char*> res;
			for (auto addr : addrs)
			{
				auto tmp = table.rawSelect(selArgs, addr);
				res.insert(res.end(), tmp.begin(), tmp.end());
			}
			return res;
		}
	}
	return tables[tableName]->select(cond);
}

int DB::doRemove
(std::string tableName, unsigned char* &blk, std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned addr)
{
	auto &table = *tables[tableName];
	unsigned tupleSize = table.tupleSize;
	int changed = 0;
	blk = bm.read(addr);
	for (unsigned char* p = blk + 8; p < blk + *(unsigned*)blk; p += tupleSize)
	{
		for (auto arg : args)
		{
			if (memcmp(arg.first, p + arg.second.first, arg.second.second) != 0)
				goto nexttime;
		}
		for (auto indexPair : tabIdx[tableName])
		{
			unsigned attr = indexPair.first;
			auto& index = indexes[indexPair.second];
			index->remove(p + table.cols[attr - 1], addr);
		}
		++changed;
		memcpy(p, p + tupleSize, *(unsigned*)blk + blk - p - tupleSize);
		*(unsigned*)blk = *(unsigned*)blk - tupleSize;
		p -= tupleSize;
	nexttime:;
	}
	if (changed)
	{
		bm.write(addr);
	}
	return changed;
}
