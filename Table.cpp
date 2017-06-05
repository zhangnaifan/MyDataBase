//#include "RawTable.h"
#include "Table.h"
#include <vector>
#include <algorithm>
#include <iostream>
#include <vector>

SeqTable::SeqTable(BufferManager & _bm, unsigned _metaAddr):RawTable(_bm, _metaAddr)
{
	unsigned char* blk = _bm.read(_metaAddr);
	unsigned char* p = blk + 28;
	int stage = 1; //1:pk, 2:numCol, 3:col_1, col_2,..., 4:blk_1, blk_2, blk_3...
	int count = 0;
	while (stage < 4)
	{
		if (p == blk + *(unsigned*)blk)
		{
			blk = _bm.read(getNextAddr(blk));
			p = blk + 8;
		}
		switch (stage)
		{
		case 1:
			searchKey = *(unsigned*)p;
			++stage;
			break;
		case 2:
			count = *(unsigned*)p;
			cols.push_back(0);
			++stage;
			break;
		case 3:
			cols.push_back(*(unsigned*)p);
			if (--count == 0)
			{
				++stage;
			}
			count = numBlk;
			break;
		case 4:
			index.push_back(*(unsigned*)p);
			if (--count == 0)
			{
				++stage;
			}
		default:
			break;
		}
		p += 4;
	}
}

SeqTable::SeqTable(BufferManager & _bm, int _tupleSize, unsigned _searchKey, std::vector<unsigned> _cols, bool _cmp)
	:searchKey(_searchKey),cols(_cols), RawTable(_bm, _tupleSize, _cmp)
{
	index.push_back(startAddr);
}

SeqTable::~SeqTable()
{
	delete[] cache;
}

int SeqTable::insert(std::map<unsigned, unsigned char*> args)
{
	if (args.find(searchKey) == args.end())
	{
		std::cerr << "INSERT ERROR: No SearchKey" << std::endl;
		return 0;
	}
	//构造元组
	format(cache, args);
	//获取插入位置
	auto pos = binarySearch(cache + cols[searchKey-1]);
	/*已存在相同的searchKey
	if (pos[0] == 0)
	{
		std::cerr << "INSERT FAIL: cannot insert a tuple with exsiting PK!" << std::endl;
		return 0;
	}
	*/
	//使用rawAdd执行插入，并更新index（如果需要）
	unsigned addr = pos[2], offset = pos[3];
	unsigned newAddr;
	if (-1 != (newAddr = rawAdd(cache, addr, offset)))
	{
		index.insert(index.begin() + pos[1] + 1, newAddr);
	}
	return 1;
}

int SeqTable::remove(std::map<unsigned, unsigned char*> args)
{
	std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> rmvArgs;
	for (auto p : args)
	{
		rmvArgs.push_back({ p.second, {cols[p.first - 1], cols[p.first] - cols[p.first-1]} });
	}
	unsigned char* bufBlk;
	if (args.find(searchKey) != args.end())
	{
		auto pos = binarySearch(args[searchKey]);
		if (pos[0] != 0)
		{
			return 0;
		}
		std::vector<unsigned> candidate{ pos[1] };
		for (unsigned i = pos[1] - 1; i != -1; --i)
		{
			unsigned addr = index[i];
			auto linPos = linearSerach(args[searchKey], cols[searchKey - 1], cols[searchKey], addr, 1);
			if (linPos.first != 0)
				break;
			candidate.push_back(i);
		}
		for (unsigned i = pos[1] + 1; i < index.size(); ++i)
		{
			unsigned addr = index[i];
			auto linPos = linearSerach(args[searchKey], cols[searchKey - 1], cols[searchKey], addr, 1);
			if (linPos.first != 0)
				break;
			candidate.push_back(i);
		}
		int res = 0;
		for (auto addr : candidate)
		{
			res += doRemove(bufBlk, rmvArgs, index[addr]);
			//磁盘块为空，回收
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
					*(unsigned*)(blk + 4) = getNextAddr(cache);
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
			res += doRemove(bufBlk, rmvArgs, index[addr]);
			//磁盘块为空，回收
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
					*(unsigned*)(blk + 4) = getNextAddr(bufBlk);
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

std::vector<unsigned char*> SeqTable::select(std::map<unsigned, unsigned char*> args)
{
	std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> selArgs;
	for (auto p : args)
	{
		selArgs.push_back({ p.second,{ cols[p.first - 1], cols[p.first] - cols[p.first-1] } });
	}
	if (args.find(searchKey) != args.end())
	{
		std::vector<unsigned char*> ret;
		auto pos = binarySearch(args[searchKey]);
		if (pos[0] != 0)
		{
			return ret;
		}
		std::vector<unsigned> candidateBlks{ pos[2] };
		for (unsigned i = pos[1] - 1; i != -1; --i)
		{
			unsigned addr = index[i];
			auto linPos = linearSerach(args[searchKey], cols[searchKey - 1], cols[searchKey], addr, 1);
			if (linPos.first != 0)
				break;
			candidateBlks.push_back(addr);
		}
		for (unsigned i = pos[1] + 1; i < index.size(); ++i)
		{
			unsigned addr = index[i];
			auto linPos = linearSerach(args[searchKey], cols[searchKey - 1], cols[searchKey], addr, 1);
			if (linPos.first != 0)
				break;
			candidateBlks.push_back(addr);
		}
		unsigned char* bufBlk;
		for (auto i : candidateBlks)
		{
			auto tmp = doSelect(bufBlk, selArgs, i);
			ret.insert(ret.end(), tmp.begin(), tmp.end());
		}
		return ret;
	}
	else
	{
		return rawSelect(selArgs);
	}
}

std::vector<unsigned> SeqTable::binarySearch(unsigned char * cond)
{
	unsigned first = 0;
	unsigned last = index.size() - 1;
	std::vector<unsigned> ret(4, -1);
	while (first <= last)
	{
		unsigned mid = first + (last - first) / 2;
		unsigned mid_addr = index[mid];
		auto res = linearSerach(cond, cols[searchKey-1], cols[searchKey], mid_addr, 1);
		ret[0] = res.first;
		ret[1] = mid;
		ret[2] = mid_addr;
		ret[3] = res.second.second;
		if (res.first == 0)
		{
			break;
		}
		if (res.first > 0)
		{
			first = mid + 1;
		}
		else if (res.second.second == 8)
		{
			if (mid == 0)
				break;
			last = mid - 1;
		}
		else
		{
			break;
		}
	}
	return ret;
}

void SeqTable::format(unsigned char* blk, std::map<unsigned, unsigned char*> args)
{
	memset(blk, 0, tupleSize);
	for (auto p : args)
	{
		memcpy(blk + cols[p.first-1], p.second, cols[p.first] - cols[p.first - 1]);
	}
}

