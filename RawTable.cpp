#include <vector>
#include <cstdlib>
#include <iostream>
#include "RawTable.h"

//假设blk大小至少为24
RawTable::RawTable(unsigned addr, BufferManager &_bm) : bm(_bm), metaAddr(addr)
{
	unsigned char* blk = bm.read(addr);
	tupleSize = *(unsigned*)(blk + 8);
	startAddr = *(unsigned*)(blk + 12);
	numBlk = *(unsigned*)(blk + 16);
}

RawTable::RawTable(BufferManager &_bm, unsigned _tupleSize) 
	: bm(_bm), tupleSize(_tupleSize), metaAddr(-1)
{
	startAddr = rand();
	numBlk = 1;
	unsigned char* newBlk = bm.get(startAddr);
	*(unsigned*)newBlk = 8;
	*(unsigned*)(newBlk + 4) = -1;
	bm.write(startAddr);
}

RawTable::~RawTable()
{
	delete[] myBuf;
}

unsigned RawTable::saveTable()
{
	if (metaAddr == -1)
	{
		metaAddr = getFreeBlockOnDisk();
	}
	unsigned char *blk = bm.get(metaAddr);
	*(unsigned*)blk = 24;
	*(unsigned*)(blk + 4) = -1;
	*(unsigned*)(blk + 8) = tupleSize;
	*(unsigned*)(blk + 12) = startAddr;
	*(unsigned*)(blk + 16) = numBlk;
	bm.write(metaAddr);
	return metaAddr;
}
/*
输入：元组和插入位置
返回：-1如果没有申请新磁盘块；否则，新的磁盘块地址
*/
unsigned RawTable::rawAdd(unsigned char * tuple, unsigned addr, unsigned offset)
{
	if (addr == -1)
	{
		addr = startAddr + numBlk - 1;
	}
	unsigned char* blk = bm.read(addr);
	if (offset == -1)
	{
		offset = *(unsigned*)blk;
	}
	if (full(blk))
	{
		//再申请一个磁盘块
		unsigned newBlkAddr = getFreeBlockOnDisk();
		//unsigned char* newBlk = bm.get(newBlkAddr);
		
		//将addr内的元组和tuple平分到新老两个磁盘块中
		//判断新的元组在哪个磁盘块中
		unsigned sum = 1 + (*(unsigned*)blk - 8) / tupleSize;
		unsigned mid =  sum / 2;
		unsigned pos = (offset - 8) / tupleSize + 1;

		//如果新的tuple在原块内
		if (pos <= mid)
		{
			//拷贝原第mid块到最后一块到newBlk中
			memcpy(myBuf + 8, blk + 8 + (mid - 1)*tupleSize, (sum - mid)*tupleSize);
			//将原来的第pos块到第mid-1块平移
			for (unsigned char* p = blk + 8 + (mid - 2)*tupleSize; p >= blk + 8 + (pos - 1)*tupleSize; p -= tupleSize)
			{
				memcpy(p + tupleSize, p, tupleSize);
			}
			//拷贝进tuple
			memcpy(blk + 8 + (pos - 1)*tupleSize, tuple, tupleSize);
		}
		else
		{
			//存入tuple+拷贝
			unsigned first = pos - (mid + 1);
			unsigned second = sum - 1 - mid - first;
			memcpy(myBuf + 8, blk + 8 + mid * tupleSize, first * tupleSize);
			memcpy(myBuf + 8 + first * tupleSize, tuple, tupleSize);
			memcpy(myBuf + 8 + (first + 1) * tupleSize, blk + 8 + (mid + first)*tupleSize, second*tupleSize);
		}

		*(unsigned*)myBuf = (sum - mid)*tupleSize + 8;
		*(unsigned*)(myBuf + 4) = getNextAddr(blk);
		*(unsigned*)blk = mid*tupleSize + 8;
		*(unsigned*)(blk + 4) = newBlkAddr;

		bm.write(addr);

		unsigned char* newBlk = bm.get(newBlkAddr);
		memcpy(newBlk, myBuf, bm.getBlkSize());
		bm.write(newBlkAddr);

		/*show the process
		std::cout << "add block of " << newBlkAddr << ":\n";
		std::cout << addr << ": ";
		for (unsigned char* p = blk + 8; p != blk + *(unsigned*)blk; p += tupleSize)
			std::cout << *(unsigned*)p << " ";
		std::cout << std::endl;

		std::cout << newBlkAddr << ": ";
		for (unsigned char* p = myBuf + 8; p != myBuf + *(unsigned*)myBuf; p += tupleSize)
			std::cout << *(unsigned*)p << " ";
		std::cout << std::endl;
		*/

		++numBlk;
		return newBlkAddr;
	}
	else
	{
		for (unsigned char* p = blk + *(unsigned*)blk - tupleSize; p >= blk + offset; p -= tupleSize)
		{
			memcpy(p + tupleSize, p, tupleSize);
		}
		memcpy(blk + offset, tuple, tupleSize);
		*(unsigned*)blk = *(unsigned*)blk + tupleSize;

		/*show the process
		std::cout << "update block of " << addr << ": ";
		for (unsigned char* p = blk + 8; p != blk + *(unsigned*)blk; p += tupleSize)
			std::cout << *(unsigned*)p << " ";
		std::cout << std::endl;
		*/
		bm.write(addr);
		return -1;
	}
}

int RawTable::rawRemove(std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned addr)
{
	unsigned char* blk;
	if (addr != -1)
	{
		return doRemove(blk, args, addr);
	}
	int res = 0;
	addr = startAddr;
	do {
		res += doRemove(blk, args, addr);
	} while ((addr = getNextAddr(blk)) != -1);
	return res;
}

int RawTable::rawUpdate(std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned char * after, unsigned addr)
{
	unsigned char* blk;
	if (addr != -1)
	{
		return doUpdate(blk, args, after, addr);
	}
	int res = 0;
	addr = startAddr;
	do {
		res += doUpdate(blk, args, after, addr);
	} while ((addr = getNextAddr(blk)) != -1);
	return res;
}

/*危险函数，必须在缓冲区被刷新之前读取数据*/
std::vector<unsigned char*> RawTable::rawSelect(std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned addr)
{
	unsigned char* blk;
	if (addr != -1)
	{
		return doSelect(blk, args, addr);
	}
	std::vector<unsigned char*> res;
	addr = startAddr;
	do {
		auto tmp = doSelect(blk, args, addr);
		res.insert(res.end(), tmp.begin(), tmp.end());
	} while ((addr = getNextAddr(blk)) != -1);
	return res;
}

int RawTable::doRemove(unsigned char* &blk, std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned addr)
{
	int changed = 0;
	blk = bm.read(addr);
	for (unsigned char* p = blk + 8; p < blk + *(unsigned*)blk; p += tupleSize)
	{
		for (auto arg : args)
		{
			if (memcmp(arg.first, p + arg.second.first, arg.second.second) != 0)
				goto nexttime;
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

int RawTable::doUpdate(unsigned char* &blk, std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned char * after, unsigned addr)
{
	int changed = 0;
	blk = bm.read(addr);
	for (unsigned char* p = blk + 8; p != blk + *(unsigned*)blk; p += tupleSize)
	{
		for (auto arg : args)
		{
			if (memcmp(arg.first, p + arg.second.first, arg.second.second) != 0)
				goto nexttime;
		}
		++changed;
		memcpy(p, after, tupleSize);
	nexttime:;
	}
	if (changed)
	{
		bm.write(addr);
	}
	return changed;
}

std::vector<unsigned char*> RawTable::doSelect(unsigned char* &blk, std::vector<std::pair<unsigned char*, std::pair<unsigned, unsigned>>> args, unsigned addr)
{
	std::vector<unsigned char*> res;
	blk = bm.read(addr);
	for (unsigned char* p = blk + 8; p != blk + *(unsigned*)blk; p += tupleSize)
	{
		for (auto arg : args)
		{
			if (memcmp(arg.first, p + arg.second.first, arg.second.second) != 0)
				goto nexttime;
		}
		res.push_back(p);
	nexttime:;
	}
	return res;
}

inline unsigned RawTable::getNextAddr(unsigned char *blk)
{
	return *(unsigned*)(blk + 4);
}

inline bool RawTable::full(unsigned char *blk)
{
	return *(unsigned*)blk + tupleSize > bm.getBlkSize();
}

inline unsigned RawTable::getFreeBlockOnDisk()
{
	return rand();
}

