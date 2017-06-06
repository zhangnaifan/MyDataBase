#include <vector>
#include <cstdlib>
#include <iostream>
#include "RawTable.h"

//����blk��С����Ϊ24
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
���룺Ԫ��Ͳ���λ��
���أ�-1���û�������´��̿飻�����µĴ��̿��ַ
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
		//������һ�����̿�
		unsigned newBlkAddr = getFreeBlockOnDisk();
		//unsigned char* newBlk = bm.get(newBlkAddr);
		
		//��addr�ڵ�Ԫ���tupleƽ�ֵ������������̿���
		//�ж��µ�Ԫ�����ĸ����̿���
		unsigned sum = 1 + (*(unsigned*)blk - 8) / tupleSize;
		unsigned mid =  sum / 2;
		unsigned pos = (offset - 8) / tupleSize + 1;

		//����µ�tuple��ԭ����
		if (pos <= mid)
		{
			//����ԭ��mid�鵽���һ�鵽newBlk��
			memcpy(myBuf + 8, blk + 8 + (mid - 1)*tupleSize, (sum - mid)*tupleSize);
			//��ԭ���ĵ�pos�鵽��mid-1��ƽ��
			for (unsigned char* p = blk + 8 + (mid - 2)*tupleSize; p >= blk + 8 + (pos - 1)*tupleSize; p -= tupleSize)
			{
				memcpy(p + tupleSize, p, tupleSize);
			}
			//������tuple
			memcpy(blk + 8 + (pos - 1)*tupleSize, tuple, tupleSize);
		}
		else
		{
			//����tuple+����
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

/*Σ�պ����������ڻ�������ˢ��֮ǰ��ȡ����*/
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

