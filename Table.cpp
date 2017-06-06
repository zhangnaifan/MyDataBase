#include "Table.h"
#include <vector>
#include <algorithm>
#include <iostream>
#include <vector>

Table::Table(unsigned _metaAddr, BufferManager & _bm):RawTable(_metaAddr, _bm)
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
				count = numBlk;
			}
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

Table::Table(BufferManager & _bm, int _tupleSize, unsigned _searchKey, std::vector<unsigned> _cols)
	:searchKey(_searchKey),cols(_cols), RawTable(_bm, _tupleSize)
{
	index.push_back(startAddr);
}

Table::~Table()
{
	delete[] cache;
}

void Table::save()
{
	/*
	�ӵ�28�ֽڿ�ʼ��
	4�ֽ�unsigned��colPk
	4�ֽ�unsigned��colNum
	���ÿ4���ֽڱ���һ����ռ���ֽ���

	����numBlk��4�ֽڣ��洢��Ӧ��index
	*/
	RawTable::save();
	unsigned addr = metaAddr;
	unsigned char* blk = bm.read(metaAddr);
	unsigned char* p = blk + 28;
	int stage = 1; //1:pk, 2:numCol, 3:col_1, col_2,..., 4:blk_1, blk_2, blk_3...
	int count = 0;
	while (stage < 4)
	{
		if (p == blk + *(unsigned*)blk)
		{
			//1�����̲�����
			if (-1 == getNextAddr(blk))
			{
				unsigned newAddr = getFreeBlockOnDisk();
				//���µ�ַ��д��
				*(unsigned*)(blk + 4) = newAddr;
				bm.write(addr);
				//������һ������
				addr = newAddr;
				blk = bm.get(addr);
				*(unsigned*)blk = 8;
				*(unsigned*)(blk + 4) = -1;
				p = blk + 8;
			}
			else
			{
				//ֱ��д��
				bm.write(addr);
				addr = getNextAddr(blk);
				blk = bm.read(addr);
				p = blk + 8;
			}
		}
		switch (stage)
		{
		case 1:
			*(unsigned*)p = searchKey;
			++stage;
			break;
		case 2:
			count = cols.size() - 1;
			*(unsigned*)p = count;
			++stage;
			break;
		case 3:
			*(unsigned*)p = *(cols.end() - count);
			if (--count == 0)
			{
				++stage;
				count = numBlk;
			}
			break;
		case 4:
			*(unsigned*)p = *(index.end() - count);
			if (--count == 0)
			{
				++stage;
			}
		default:
			break;
		}
		p += 4;
	}
	bm.write(addr);
}

void Table::dropAll()
{
	//������ݴ��̿�
	for (auto i : index)
	{
		bm.drop(i);
	}
	//���meta���̿�
	unsigned char* blk;
	unsigned addr = metaAddr;
	while (addr != -1)
	{
		blk = bm.read(addr);
		unsigned newAddr = getNextAddr(blk);
		bm.drop(addr);
		addr = newAddr;
	}
}

void Table::format(unsigned char * blk, std::map<unsigned, unsigned char*> args)
{
	memset(blk, 0, tupleSize);
	for (auto p : args)
	{
		memcpy(blk + cols[p.first - 1], p.second, cols[p.first] - cols[p.first - 1]);
	}
}


SeqTable::SeqTable(unsigned _metaAddr, BufferManager & _bm):Table(_metaAddr, _bm)
{
	unsigned char* blk = bm.read(_metaAddr);
	cmp = *(bool*)(blk + 20);
}

SeqTable::SeqTable(BufferManager & _bm, int _tupleSize, unsigned _searchKey, std::vector<unsigned> _cols, bool _cmp)
	:Table(_bm, _tupleSize, _searchKey, _cols), cmp(_cmp)
{
}

std::pair<unsigned, std::pair<unsigned, unsigned>> SeqTable::insert(std::map<unsigned, unsigned char*> args, bool distinct)
{
	if (args.find(searchKey) == args.end())
	{
		std::cerr << "INSERT ERROR: No SearchKey" << std::endl;
		return { 0,{} };
	}
	//����Ԫ��
	format(cache, args);
	//��ȡ����λ��
	auto pos = binarySearch(cache + cols[searchKey-1]);
	//�Ѵ�����ͬ��searchKey
	if (distinct && pos[0] == 0)
	{
		std::cerr << "INSERT FAIL: cannot insert a tuple with exsiting key!" << std::endl;
		return { 0,{} };
	}
	//ʹ��rawAddִ�в��룬������index�������Ҫ��
	unsigned addr = pos[2], offset = pos[3];
	unsigned newAddr;
	auto rawAddResult = rawAdd(cache, addr, offset);
	if (-1 != (newAddr = rawAddResult.first))
	{
		index.insert(index.begin() + pos[1] + 1, newAddr);
	}
	return { 1, rawAddResult.second };
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
			auto linPos = linearSerach(args[searchKey], cols[searchKey - 1], cols[searchKey] - cols[searchKey - 1], addr, 1);
			if (linPos.first != 0)
				break;
			candidate.push_back(i);
		}
		for (unsigned i = pos[1] + 1; i < index.size(); ++i)
		{
			unsigned addr = index[i];
			auto linPos = linearSerach(args[searchKey], cols[searchKey - 1], cols[searchKey] - cols[searchKey - 1], addr, 1);
			if (linPos.first != 0)
				break;
			candidate.push_back(i);
		}
		int res = 0;
		for (auto addr : candidate)
		{
			res += doRemove(bufBlk, rmvArgs, index[addr]);
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
			auto linPos = linearSerach(args[searchKey], cols[searchKey - 1], cols[searchKey] - cols[searchKey - 1], addr, 1);
			if (linPos.first != 0)
				break;
			candidateBlks.push_back(addr);
		}
		for (unsigned i = pos[1] + 1; i < index.size(); ++i)
		{
			unsigned addr = index[i];
			auto linPos = linearSerach(args[searchKey], cols[searchKey - 1], cols[searchKey] - cols[searchKey - 1], addr, 1);
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

void SeqTable::save()
{
	Table::save();
	unsigned char* blk = bm.read(metaAddr);
	*(bool*)(blk + 20) = cmp;
	bm.write(metaAddr);
}

std::pair<int, std::pair<unsigned, unsigned>> SeqTable::linearSerach(unsigned char* cond, unsigned beg, unsigned len, unsigned from, unsigned steps)
{
	if (from == -1)
	{
		from = startAddr;
	}
	unsigned addr = from; //��ǰ��ַ
	unsigned char *p = nullptr; //��ǰ����ָ��
	unsigned char* blk = nullptr; //��ǰ��
	for (unsigned i = 0; addr != -1 && i < steps; addr = getNextAddr(blk))
	{
		blk = bm.read(addr);
		for (p = blk + 8; p != blk + *(unsigned*)blk; p += tupleSize)
		{
			//ע��С�˴洢����!!!!!
			//int res = memcmp(cond, p + beg, end - beg);
			int res = cmp ? memcmp(cond, p + beg, len) : *(unsigned*)cond - *(unsigned*)(p + beg);
			if (res <= 0)
			{
				return { res,{ addr, p - blk } };
			}
		}
		if (getNextAddr(blk) == -1 || ++i == steps)
			break;
	}
	return { 1,{ addr, p - blk } };
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
		auto res = linearSerach(cond, cols[searchKey-1], cols[searchKey] - cols[searchKey - 1], mid_addr, 1);
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
