#include "extmem.h"
#include "BufferManager.h"
#include <unordered_map>
#include <list>
#include <iostream>

BufferManager::BufferManager(size_t bufSize, size_t blkSize)
{
	initBuffer(bufSize, blkSize, &buffer);
}
BufferManager::~BufferManager()
{
	for (auto p : LUT)
		if (p.second.second)
			writeBlockToDisk(p.second.first, p.first, &buffer);
	freeBuffer(&buffer);
}
bool BufferManager::full()
{
	return buffer.bufSize < (1+LUT.size())*(1 + buffer.blkSize);
}
bool BufferManager::write(unsigned addr)
{
	if (LUT.find(addr) == LUT.end())
	{
		std::cerr << "ERROR: " << addr << " has been swapped\n" << std::endl;
		return false;
	}
	return LUT[addr].second = true;
}
int BufferManager::getNumIO()
{
	return buffer.numIO;
}
unsigned BufferManager::getBlkSize()
{
	return buffer.blkSize;
}


FIFOBufMgr::FIFOBufMgr(size_t bufSize, size_t blkSize) :BufferManager(bufSize, blkSize) {}
unsigned char* FIFOBufMgr::get(unsigned addr)
{
	return doGetBlk(false, addr);
}
unsigned char* FIFOBufMgr::read(unsigned addr)
{
	return doGetBlk(true, addr);
}
void FIFOBufMgr::swap()
{
	if (q.empty())	return;
	unsigned out = q.front();
	//�Ƴ�����
	q.pop_front();
	//д�ش���
	if (LUT[out].second)
	{
		writeBlockToDisk(LUT[out].first, out, &buffer);
	}
	//�ͷſռ�
	freeBlockInBuffer(LUT[out].first, &buffer);
	//ɾȥ��¼
	LUT.erase(out);
	pos.erase(out);
}
unsigned char* FIFOBufMgr::doGetBlk(bool type, unsigned addr)
{
	//�ڻ�����
	if (LUT.find(addr) != LUT.end())
	{
		q.erase(pos[addr]);
		q.push_back(addr);
		pos[addr] = --q.end();
		return LUT[addr].first;
	}
	//���ڻ����У��������������滻
	if (full())
	{
		swap();
	}
	//���µ��ȶ���
	q.push_back(addr);
	pos[addr] = --q.end();
	//�Ӵ��̶��뻺���������һ�´��̿�
	unsigned char* blk = type ? readBlockFromDisk(addr, &buffer) : getNewBlockInBuffer(&buffer);
	//����һ����¼
	LUT[addr] = { blk, false };
	return blk;
}


LRUBufMgr::LRUBufMgr(size_t bufSize, size_t blkSize) :BufferManager(bufSize, blkSize) {}
unsigned char * LRUBufMgr::read(unsigned addr)
{
	return doGetBlk(true, addr);
}
unsigned char* LRUBufMgr::get(unsigned addr)
{
	return doGetBlk(false, addr);
}
void LRUBufMgr::swap()
{
	unsigned out = queue[minFreq].front();
	//д�ش���
	if (LUT[out].second)
	{
		writeBlockToDisk(LUT[out].first, out, &buffer);
	}
	//�ͷŻ�����
	freeBlockInBuffer(LUT[out].first, &buffer);
	//ɾȥLUT�еļ�¼
	LUT.erase(out);
	//ɾȥfreq/pos/queue�еļ�¼
	freq.erase(out);
	pos.erase(out);
	queue[minFreq].pop_front(); //minFreq���ɺ���������£�=1��
	//std::cout << "**************swap " << out << std::endl;
}
unsigned char* LRUBufMgr::doGetBlk(bool type, unsigned addr)
{
	//��������
	if (LUT.find(addr) != LUT.end())
	{
		//���µ������ݽṹ
		int fr = freq[addr]++;
		queue[fr].erase(pos[addr]);
		queue[fr + 1].push_back(addr);
		pos[addr] = --queue[fr + 1].end();
		if (queue[fr].empty() && fr == minFreq)
		{
			minFreq = fr + 1;
		}
		return LUT[addr].first;
	}
	//���ڻ�������
	if (full()) //��Ҫ�����滻
	{
		swap();
	}
	freq[addr] = 1;
	minFreq = 1;
	queue[1].push_back(addr);
	pos[addr] = --queue[1].end();
	unsigned char *blk = type ? readBlockFromDisk(addr, &buffer) : getNewBlockInBuffer(&buffer);
	LUT[addr] = { blk, false };
	return blk;
}


MRUBufMgr::MRUBufMgr(size_t bufSize, size_t blkSize) :BufferManager(bufSize, blkSize) {}
unsigned char * MRUBufMgr::read(unsigned addr)
{
	return doGetBlk(true, addr);
}
unsigned char * MRUBufMgr::get(unsigned addr)
{
	return  doGetBlk(false, addr);
}
void MRUBufMgr::swap()
{
	unsigned out = queue[maxFreq].front();
	//д�ش���
	if (LUT[out].second)
	{
		writeBlockToDisk(LUT[out].first, out, &buffer);
	}
	//�ͷŻ�����
	freeBlockInBuffer(LUT[out].first, &buffer);
	//ɾȥLUT�еļ�¼
	LUT.erase(out);
	//ɾȥfreq/pos/queue�еļ�¼
	freq.erase(out);
	pos.erase(out);
	queue[maxFreq].pop_front();
	//����maxFreq
	for (; maxFreq > 0 && queue[maxFreq].empty(); --maxFreq);
	//std::cout << "**************swap " << out << std::endl;
}
unsigned char * MRUBufMgr::doGetBlk(bool type, unsigned addr)
{
	//��������
	if (LUT.find(addr) != LUT.end())
	{
		//���µ������ݽṹ
		int fr = freq[addr]++;
		queue[fr].erase(pos[addr]);
		queue[fr + 1].push_back(addr);
		pos[addr] = --queue[fr + 1].end();
		if (fr == maxFreq)
		{
			++maxFreq;
		}
		return LUT[addr].first;
	}
	//���ڻ�������
	if (full()) //��Ҫ�����滻
	{
		swap();
	}
	freq[addr] = 1;
	if (!maxFreq)
		maxFreq = 1;
	queue[1].push_back(addr);
	pos[addr] = --queue[1].end();
	unsigned char *blk = type ? readBlockFromDisk(addr, &buffer) : getNewBlockInBuffer(&buffer);
	LUT[addr] = { blk, false };
	return blk;
}
